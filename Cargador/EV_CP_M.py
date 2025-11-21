"""
EV_CP_Monitor_kafka.py - Monitor del CP (Kafka-based)
- Se conecta vía Kafka y registra el CP en la CENTRAL al inicio
- Escucha estados publicados por el Engine (topic_cp_status)
- Permite simular fault/repair via console and sends FAULT messages to central
"""

import json
import time
import threading
import yaml
import os
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from datetime import datetime
from Extras.utils import resolve_broker

BASE = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(BASE, "..", "config", "config.yaml"), "r") as f:
    config = yaml.safe_load(f)

BROKER = resolve_broker(config, "CP_MONITOR")
TOPIC_CP_STATUS = config["kafka"]["topic_cp_status"]
TOPIC_CP_DATA = config["kafka"]["topic_cp_data"]

class Monitor:
    def __init__(self, cp_id):
        self.cp_id = cp_id
        self.producer = None
        self.consumer = None
        self._kafka_lock = threading.Lock()
        self.last_heartbeat = None
        self.engine_state = "unknown"  # ok | faulty | disconnected | unknown
        self._start_ts = time.time()
        self._fault_sent = False
        self._connect_kafka()
        self.is_faulty = False
        # Register on start
        reg = {"type":"REGISTER", "cp_id": self.cp_id, "info": {"monitor":"kafka_monitor"}}
        self.producer.send(TOPIC_CP_STATUS, reg); self.producer.flush()

    def _connect_kafka(self):
        with self._kafka_lock:
            while True:
                try:
                    print(f"[Monitor {self.cp_id}] Conectando con Kafka en {BROKER}...")
                    self.producer = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
                    self.consumer = KafkaConsumer(TOPIC_CP_STATUS, TOPIC_CP_DATA, bootstrap_servers=BROKER, value_deserializer=lambda v: json.loads(v.decode("utf-8")), group_id=f"monitor_{self.cp_id}", auto_offset_reset="earliest", consumer_timeout_ms=1000)
                    print(f"[Monitor {self.cp_id}] Kafka conectado.")
                    break
                except (KafkaError, NoBrokersAvailable) as e:
                    print(f"[Monitor {self.cp_id}] Error conectando a Kafka: {e}. Reintentando en 2s...")
                    time.sleep(2)

    def start(self):
        threading.Thread(target=self.listen_status, daemon=True).start()
        threading.Thread(target=self.watchdog_loop, daemon=True).start()
        try:
            self.print_menu()
            while True:
                cmd = input(f"\n[Monitor {self.cp_id}] > ").strip().lower()
                if cmd in ("status","s"):
                    self.print_status()
                elif cmd in ("q","quit","exit"):
                    break
                else:
                    print("Comandos: status | q")
                self.print_menu()
        except KeyboardInterrupt:
            pass
        print("Monitor exiting.")

    def listen_status(self):
        while True:
            try:
                for msg in self.consumer:
                    data = msg.value
                    # Only care for messages for this cp
                    if data.get("cp_id") != self.cp_id:
                        continue
                    mtype = data.get("type")
                    if mtype in ("HEARTBEAT","STATUS"):
                        self.last_heartbeat = time.time()
                        if data.get("status") == "faulty":
                            self.engine_state = "faulty"
                            self._fault_sent = True
                        else:
                            self.engine_state = "ok"
                            self._fault_sent = False
                    # Logs reducidos: solo eventos relevantes
                    if mtype in ("FAULT","CHARGE_COMPLETE"):
                        print(f"[Monitor {self.cp_id}] Status message: {data}")
            except (KafkaError, NoBrokersAvailable) as e:
                print(f"[Monitor {self.cp_id}] Error en consumer Kafka: {e}. Reintentando conexión...")
                time.sleep(1)
                self._connect_kafka()
            except Exception as e:
                print(f"[Monitor {self.cp_id}] Error inesperado en listen_status: {e}")
                time.sleep(1)

    def send_fault(self):
        fault = {"type":"FAULT", "cp_id": self.cp_id, "timestamp": datetime.now().isoformat()}
        self.producer.send(TOPIC_CP_STATUS, fault); self.producer.flush()

    def send_repair(self):
        repair = {"type":"STATUS", "cp_id": self.cp_id, "status":"ok", "timestamp": datetime.now().isoformat()}
        self.producer.send(TOPIC_CP_STATUS, repair); self.producer.flush()

    def watchdog_loop(self):
        # Si no recibe heartbeats por 3 intervalos (~15s) informa avería
        while True:
            time.sleep(5)
            now = time.time()
            if self.last_heartbeat is None:
                # Si nunca hemos recibido latido y ya pasó el umbral, marcamos avería
                if not self._fault_sent and (now - self._start_ts) > 15:
                    print(f"[Monitor {self.cp_id}] Sin heartbeats iniciales: enviando FAULT")
                    self.send_fault()
                    self.engine_state = "faulty"
                    self._fault_sent = True
                continue
            if now - self.last_heartbeat > 15:
                print(f"[Monitor {self.cp_id}] Heartbeat perdido: enviando FAULT")
                self.send_fault()
                self.last_heartbeat = now
                self.engine_state = "disconnected"
                self._fault_sent = True

    def compute_status(self):
        monitor_ok = True  # estamos vivos si llega aquí
        engine_ok = self.engine_state == "ok"
        if monitor_ok and engine_ok:
            return "Conectado"
        if monitor_ok and not engine_ok:
            # si está faulty o desconectado
            return "Avería" if self.engine_state == "faulty" else "Desconectado"
        # monitor KO
        if not monitor_ok and engine_ok:
            return "Desconectado"
        return "Desconectado"

    def print_status(self):
        status = self.compute_status()
        hb_txt = "n/a"
        if self.last_heartbeat:
            hb_txt = f"hace {time.time()-self.last_heartbeat:.1f}s"
        print(f"[Monitor {self.cp_id}] Estado: {status} | Último heartbeat: {hb_txt}")

    def print_menu(self):
        status = self.compute_status()
        print("\n" + "=" * 60)
        print(f"Monitor {self.cp_id} - Menú | Estado CP: {status}")
        print("=" * 60)
        print("Comandos:")
        print("  status  - Mostrar estado actual")
        print("  q       - Salir")
        print("=" * 60)

def main(cp_id):
    monitor = Monitor(cp_id)
    monitor.start()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso: python EV_CP_Monitor_kafka.py <CP_ID>")
        sys.exit(1)
    main(sys.argv[1])
