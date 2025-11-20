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
from kafka.errors import KafkaError
from datetime import datetime

BASE = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(BASE, "..", "config", "config.yaml"), "r") as f:
    config = yaml.safe_load(f)

BROKER = config["kafka"]["broker"]
TOPIC_CP_STATUS = config["kafka"]["topic_cp_status"]
TOPIC_CP_DATA = config["kafka"]["topic_cp_data"]

class Monitor:
    def __init__(self, cp_id):
        self.cp_id = cp_id
        self.producer = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
        self.consumer = KafkaConsumer(TOPIC_CP_STATUS, TOPIC_CP_DATA, bootstrap_servers=BROKER, value_deserializer=lambda v: json.loads(v.decode("utf-8")), group_id=f"monitor_{cp_id}", auto_offset_reset="earliest", consumer_timeout_ms=1000)
        self.is_faulty = False
        # Register on start
        reg = {"type":"REGISTER", "cp_id": self.cp_id, "info": {"monitor":"kafka_monitor"}}
        self.producer.send(TOPIC_CP_STATUS, reg); self.producer.flush()

    def start(self):
        threading.Thread(target=self.listen_status, daemon=True).start()
        print(f"[Monitor {self.cp_id}] Started. Listening for status updates. Type 'fault' or 'repair' to simulate.")
        try:
            while True:
                cmd = input(f"[Monitor {self.cp_id}] > ").strip().lower()
                if cmd == "fault":
                    self.send_fault()
                elif cmd == "repair":
                    self.send_repair()
                elif cmd in ("q","quit","exit"):
                    break
                else:
                    print("Comandos: fault | repair | q")
        except KeyboardInterrupt:
            pass
        print("Monitor exiting.")

    def listen_status(self):
        for msg in self.consumer:
            data = msg.value
            # Only care for messages for this cp
            if data.get("cp_id") != self.cp_id:
                continue
            mtype = data.get("type")
            print(f"[Monitor {self.cp_id}] Status message: {data}")

    def send_fault(self):
        fault = {"type":"FAULT", "cp_id": self.cp_id, "timestamp": datetime.now().isoformat()}
        self.producer.send(TOPIC_CP_STATUS, fault); self.producer.flush()
        print(f"[Monitor {self.cp_id}] Sent FAULT to CENTRAL")

    def send_repair(self):
        repair = {"type":"STATUS", "cp_id": self.cp_id, "status":"ok", "timestamp": datetime.now().isoformat()}
        self.producer.send(TOPIC_CP_STATUS, repair); self.producer.flush()
        print(f"[Monitor {self.cp_id}] Sent REPAIR/OK to CENTRAL")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Uso: python EV_CP_Monitor_kafka.py <CP_ID>")
        sys.exit(1)
    m = Monitor(sys.argv[1])
    m.start()
