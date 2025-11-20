"""
EV_CP_E_kafka.py - CP Engine simplified and Kafka-based.
- Listens for commands from CENTRAL on topic_cp_commands
- Publishes telemetry to topic_cp_data and status to topic_cp_status
"""

import time
import json
import threading
import yaml
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from datetime import datetime
try:
    with open('config/config.yaml', 'r') as f:
        print("Archivo de configuracion encontrado")
        config = yaml.safe_load(f)
except FileNotFoundError:
    print("Usando ruta alternativa para archivo de configuracion")
    with open("../config/config.yaml", "r") as f:
        config = yaml.safe_load(f)

BROKER = config["kafka"]["broker"]
TOPIC_CP_COMMANDS = config["kafka"]["topic_cp_commands"]
TOPIC_CP_DATA = config["kafka"]["topic_cp_data"]
TOPIC_CP_STATUS = config["kafka"]["topic_cp_status"]

class Engine:
    def __init__(self, cp_id):
        self.cp_id = cp_id
        self.cfg = config["cps"].get(cp_id, {})
        self.price = self.cfg.get("price_per_kwh", 0.25)
        self.heartbeat_interval = self.cfg.get("heartbeat_interval", 5)
        self.is_faulty = False
        self.is_charging = False
        self.current_driver = None
        self.total_energy = 0.0
        self.total_cost = 0.0
        self.session_start = None

        self.producer = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
        self.consumer = KafkaConsumer(TOPIC_CP_COMMANDS, bootstrap_servers=BROKER, value_deserializer=lambda v: json.loads(v.decode("utf-8")), group_id=f"cp_{cp_id}", auto_offset_reset="earliest", consumer_timeout_ms=1000)

    def start(self):
        # Register at central via sending REGISTER message
        reg = {"type":"REGISTER", "cp_id": self.cp_id, "info": {"price": self.price}}
        self.producer.send(TOPIC_CP_STATUS, reg); self.producer.flush()
        # Start threads
        threading.Thread(target=self.listen_commands, daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        print(f"[{self.cp_id}] Engine started. Listening for commands...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"[{self.cp_id}] Stopping engine.")

    def listen_commands(self):
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

    def heartbeat_loop(self):
        while True:
            try:
                status = {"type":"HEARTBEAT", "cp_id": self.cp_id, "status": ("faulty" if self.is_faulty else "ok"), "timestamp": datetime.now().isoformat()}
                self.producer.send(TOPIC_CP_STATUS, status); self.producer.flush()
                time.sleep(self.heartbeat_interval)
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
        print(f"[{self.cp_id}] Charging started for driver {driver_id}")

    def charging_loop(self):
        battery_capacity = 50.0
        charge_duration = config["simulation"].get("charge_duration", 120)
        interval = config["simulation"].get("charge_interval", 1)
        energy_per_sec = battery_capacity / charge_duration
        while self.is_charging:
            if self.is_faulty:
                print(f"[{self.cp_id}] Fault detected - stopping")
                self.stop_charging()
                break
            energy_inc = energy_per_sec * interval
            cost_inc = energy_inc * self.price
            self.total_energy += energy_inc
            self.total_cost += cost_inc
            duration = int((datetime.now() - self.session_start).total_seconds())
            # send telemetry
            data = {"type":"CHARGE_DATA","cp_id": self.cp_id, "driver_id": self.current_driver, "energy": round(self.total_energy,3), "cost": round(self.total_cost,2), "duration": duration, "timestamp": datetime.now().isoformat()}
            self.producer.send(TOPIC_CP_DATA, data); self.producer.flush()
            print(f"[{self.cp_id}] Telemetry: {data}")
            if self.total_energy >= battery_capacity:
                # complete
                complete = {"type":"CHARGE_COMPLETE","cp_id": self.cp_id, "driver_id": self.current_driver, "energy": round(self.total_energy,3), "cost": round(self.total_cost,2)}
                self.producer.send(TOPIC_CP_DATA, complete); self.producer.flush()
                self.stop_charging()
                break
            time.sleep(interval)

    def stop_charging(self):
        if not self.is_charging:
            return
        self.is_charging = False
        print(f"[{self.cp_id}] Charging stopped. Final energy: {self.total_energy:.3f} kWh, cost: {self.total_cost:.2f} €")
        # send final status (already sent in complete)
        self.current_driver = None

    def cancel_charging(self):
        if self.is_charging:
            self.is_charging = False
            print(f"[{self.cp_id}] Charging cancelled by CENTRAL/Driver")
            cancel = {"type":"CHARGE_COMPLETE","cp_id": self.cp_id, "driver_id": self.current_driver, "energy": round(self.total_energy,3), "cost": round(self.total_cost,2), "cancelled": True}
            self.producer.send(TOPIC_CP_DATA, cancel); self.producer.flush()
            self.current_driver = None

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso: python EV_CP_E_kafka.py <CP_ID>")
        sys.exit(1)
    cp = Engine(sys.argv[1])
    cp.start()
