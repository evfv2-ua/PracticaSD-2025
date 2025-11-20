"""
EV_CP_E_kafka.py - CP Engine Kafka-based.
- Escucha comandos de CENTRAL (topic_cp_commands)
- Publica telemetría en topic_cp_data y estados en topic_cp_status
- Incluye menú local para simular enchufe/desenchufe y fallos
"""

import time
import json
import threading
import yaml
import os
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from datetime import datetime
from Extras.utils import resolve_broker
try:
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
except FileNotFoundError:
    with open("../config/config.yaml", "r") as f:
        config = yaml.safe_load(f)

BROKER = resolve_broker(config, "CP_ENGINE")
TOPIC_CP_COMMANDS = config["kafka"]["topic_cp_commands"]
TOPIC_CP_DATA = config["kafka"]["topic_cp_data"]
TOPIC_CP_STATUS = config["kafka"]["topic_cp_status"]

class Engine:
    def __init__(self, cp_id):
        self.cp_id = cp_id
        self.cfg = config["cps"].get(cp_id, {})
        self.price = self.cfg.get("price_per_kwh", 0.25)
        self.heartbeat_interval = self.cfg.get("heartbeat_interval", 5)
        self.battery_capacity = config["simulation"].get("battery_capacity", 50.0)
        self.is_faulty = False
        self.is_charging = False
        self.is_paused = False
        self.current_driver = None
        self.total_energy = 0.0
        self.total_cost = 0.0
        self.session_start = None
        self.producer = None
        self.consumer = None
        self._kafka_lock = threading.Lock()
        self._stop = threading.Event()
        self._connect_kafka()

    def _connect_kafka(self):
        # Centraliza la conexión y reintenta hasta que el broker esté disponible.
        with self._kafka_lock:
            while True:
                try:
                    self.producer = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
                    self.consumer = KafkaConsumer(TOPIC_CP_COMMANDS, bootstrap_servers=BROKER, value_deserializer=lambda v: json.loads(v.decode("utf-8")), group_id=f"cp_{self.cp_id}", auto_offset_reset="earliest", consumer_timeout_ms=1000)
                    break
                except (KafkaError, NoBrokersAvailable) as e:
                    print(f"[{self.cp_id}] Error conectando a Kafka: {e}. Reintentando en 2s...")
                    time.sleep(2)

    def start(self):
        # Register at central via sending REGISTER message
        reg = {"type":"REGISTER", "cp_id": self.cp_id, "info": {"price": self.price}}
        self.producer.send(TOPIC_CP_STATUS, reg); self.producer.flush()
        # Start threads
        threading.Thread(target=self.listen_commands, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        threading.Thread(target=self.console_loop, daemon=True).start()
        print(f"[{self.cp_id}] Engine listo. Escuchando comandos y heartbeats.")
        try:
            while not self._stop.is_set():
                time.sleep(0.5)
        except KeyboardInterrupt:
            print(f"[{self.cp_id}] Stopping engine.")
        self._stop.set()

    def listen_commands(self):
        while True:
            try:
                for msg in self.consumer:
                    data = msg.value
                    # filter by cp_id if message targeted
                    target = data.get("cp_id")
                    if target and target != self.cp_id:
                        continue
                    mtype = data.get("type")
                    if mtype == "START_CHARGE":
                        driver_id = data.get("driver_id")
                        self.start_charging(driver_id)
                    elif mtype == "CANCEL_CHARGE":
                        if data.get("cp_id") and data.get("cp_id") == self.cp_id:
                            self.cancel_charging()
                    elif mtype == "STOP_CHARGE":
                        self.stop_charging()
                    elif mtype == "PAUSE_CHARGE":
                        self.pause_charging()
                    elif mtype == "RESUME_CHARGE":
                        self.resume_charging()
            except (KafkaError, NoBrokersAvailable) as e:
                print(f"[{self.cp_id}] Error en consumer Kafka: {e}. Reintentando conexión...")
                time.sleep(1)
                self._connect_kafka()
            except Exception as e:
                print(f"[{self.cp_id}] Error inesperado en listen_commands: {e}")
                time.sleep(1)

    def heartbeat_loop(self):
        while True:
            try:
                status = {"type":"HEARTBEAT", "cp_id": self.cp_id, "status": ("faulty" if self.is_faulty else "ok"), "timestamp": datetime.now().isoformat()}
                self.producer.send(TOPIC_CP_STATUS, status); self.producer.flush()
                time.sleep(self.heartbeat_interval)
            except (KafkaError, NoBrokersAvailable) as e:
                print(f"[{self.cp_id}] Heartbeat no enviado: {e}. Reintentando conexión...")
                time.sleep(1)
                self._connect_kafka()
            except Exception as e:
                print(f"[{self.cp_id}] Heartbeat error: {e}")
                time.sleep(2)

    def start_charging(self, driver_id):
        if self.is_faulty:
            print(f"[{self.cp_id}] Cannot start charge: faulty")
            return
        if self.is_charging:
            print(f"[{self.cp_id}] Already charging")
            return
        self.current_driver = driver_id
        self.is_charging = True
        self.total_energy = 0.0
        self.total_cost = 0.0
        self.session_start = datetime.now()
        threading.Thread(target=self.charging_loop, daemon=True).start()

    def charging_loop(self):
        charge_duration = config["simulation"].get("charge_duration", 120)
        interval = config["simulation"].get("charge_interval", 1)
        energy_per_sec = self.battery_capacity / charge_duration
        while self.is_charging:
            if self.is_faulty:
                self.stop_charging()
                break
            if self.is_paused:
                time.sleep(1)
                continue
            energy_inc = energy_per_sec * interval
            cost_inc = energy_inc * self.price
            self.total_energy += energy_inc
            self.total_cost += cost_inc
            duration = int((datetime.now() - self.session_start).total_seconds())
            # send telemetry
            data = {"type":"CHARGE_DATA","cp_id": self.cp_id, "driver_id": self.current_driver, "energy": round(self.total_energy,3), "cost": round(self.total_cost,2), "duration": duration, "timestamp": datetime.now().isoformat()}
            self.producer.send(TOPIC_CP_DATA, data); self.producer.flush()
            if self.total_energy >= self.battery_capacity:
                # complete
                complete = {"type":"CHARGE_COMPLETE","cp_id": self.cp_id, "driver_id": self.current_driver, "energy": round(self.total_energy,3), "cost": round(self.total_cost,2)}
                self.producer.send(TOPIC_CP_DATA, complete); self.producer.flush()
                self.stop_charging(send_complete=False)
                break
            time.sleep(interval)

    def stop_charging(self, send_complete=True):
        if not self.is_charging:
            return
        self.is_paused = False
        self.is_charging = False
        if send_complete and self.current_driver:
            stop_msg = {"type":"CHARGE_COMPLETE","cp_id": self.cp_id, "driver_id": self.current_driver, "energy": round(self.total_energy,3), "cost": round(self.total_cost,2), "cancelled": True, "reason": "stopped"}
            self.producer.send(TOPIC_CP_DATA, stop_msg); self.producer.flush()
        self.current_driver = None

    def cancel_charging(self):
        if self.is_charging:
            self.is_charging = False
            print(f"[{self.cp_id}] Charging cancelled by CENTRAL/Driver")
            cancel = {"type":"CHARGE_COMPLETE","cp_id": self.cp_id, "driver_id": self.current_driver, "energy": round(self.total_energy,3), "cost": round(self.total_cost,2), "cancelled": True}
            self.producer.send(TOPIC_CP_DATA, cancel); self.producer.flush()
            self.current_driver = None

    def pause_charging(self):
        if self.is_charging and not self.is_paused:
            self.is_paused = True

    def resume_charging(self):
        if self.is_charging and self.is_paused:
            self.is_paused = False

    def console_loop(self):
        def print_menu():
            print("\n" + "=" * 60)
            status = "FAULTY" if self.is_faulty else ("PAUSED" if self.is_paused else ("CHARGING" if self.is_charging else "IDLE"))
            driver_txt = f" (Driver {self.current_driver})" if self.is_charging and self.current_driver else ""
            print(f"CP Engine {self.cp_id} - Menú | Estado: {status}{driver_txt}")
            print("=" * 60)
            print("Comandos:")
            print("  1 - Iniciar carga (start <driver_id>)")
            print("  2 - Pausar carga")
            print("  3 - Reanudar carga")
            print("  4 - Detener carga")
            print("  5 - Marcar fallo")
            print("  6 - Reparar (estado OK)")
            print("  7 - Mostrar estado")
            print("  q - Salir")
            print("=" * 60)
        print_menu()
        while not self._stop.is_set():
            try:
                cmd = input(f"[{self.cp_id}] > ").strip().lower()
            except EOFError:
                break
            if not cmd:
                continue
            parts = cmd.split()
            action = parts[0]
            if action == "1" or (action == "start" and len(parts) > 1):
                driver = parts[1] if len(parts) > 1 else input("Driver ID: ").strip().upper()
                self.start_charging(driver)
            elif action in ("2","pause"):
                self.pause_charging()
            elif action in ("3","resume"):
                self.resume_charging()
            elif action in ("4","stop"):
                self.stop_charging()
            elif action in ("5","fault"):
                self.is_faulty = True
                print(f"[{self.cp_id}] Marcado como faulty")
            elif action in ("6","repair"):
                self.is_faulty = False
                print(f"[{self.cp_id}] Marcado como ok")
            elif action in ("7","status"):
                print(f"[{self.cp_id}] estado -> charging={self.is_charging} paused={self.is_paused} faulty={self.is_faulty} driver={self.current_driver}")
            elif action in ("q","quit","exit"):
                self._stop.set()
                break
            else:
                print("Comando no reconocido.")
            print_menu()

def main(cp_id):
    engine = Engine(cp_id)
    engine.start()

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso: python EV_CP_E_kafka.py <CP_ID>")
        sys.exit(1)
    main(sys.argv[1])
