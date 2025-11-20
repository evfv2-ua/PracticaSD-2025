"""
EV_Central.py - Central simplificada para la práctica EVCharging (Kafka JSON based)
- Escucha peticiones desde drivers (topic_central)
- Mantiene un registro simple de CPs (registrations desde monitor)
- Autoriza peticiones, envía comandos a CP y notifica drivers
"""

import json
import time
import threading
import yaml
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

with open("config/config.yaml","r") as f:
    config = yaml.safe_load(f)

BROKER = config["kafka"]["broker"]
TOPIC_CENTRAL = config["kafka"]["topic_central"]      # driver -> central
TOPIC_DRIVER = config["kafka"]["topic_driver"]        # central -> driver
TOPIC_CP_COMMANDS = config["kafka"]["topic_cp_commands"]  # central -> cp
TOPIC_CP_DATA = config["kafka"]["topic_cp_data"]      # cp -> central (telemetria)
TOPIC_CP_STATUS = config["kafka"]["topic_cp_status"]  # cp -> monitor/central (estado)

producer = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
consumer_driver = KafkaConsumer(TOPIC_CENTRAL, bootstrap_servers=BROKER, value_deserializer=lambda v: json.loads(v.decode("utf-8")), auto_offset_reset="earliest", group_id="central_drivers")
consumer_cp = KafkaConsumer(TOPIC_CP_DATA, TOPIC_CP_STATUS, bootstrap_servers=BROKER, value_deserializer=lambda v: json.loads(v.decode("utf-8")), auto_offset_reset="earliest", group_id="central_cps")

# Registro de CPs: cp_id -> info dict {registrado: bool, last_seen: ts, info: {}}
CP_REGISTRY = {}

def handle_driver_messages():
    print("[CENTRAL] Driver consumer funcionando.")
    for msg in consumer_driver:
        data = msg.value
        print("[CENTRAL] Recibido desde driver:", data)
        mtype = data.get("type")
        if mtype == "REQUEST_CHARGE":
            driver_id = data.get("driver_id")
            cp_id = data.get("cp_id")
            # Validacion basica: check registry
            cp_info = CP_REGISTRY.get(cp_id)
            if not cp_info or not cp_info.get("registered", False):
                # respond denied - CP not available
                resp = {"type":"AUTH_DENIED", "driver_id": driver_id, "reason":"CP_not_registered"}
                producer.send(TOPIC_DRIVER, resp)
                producer.flush()
                print(f"[CENTRAL] Denied request from {driver_id} for {cp_id}: CP not registered")
                continue
            # If CP registered and not faulty, authorize and forward to CP
            if cp_info.get("status") == "ok":
                # send AUTH_OK to driver
                resp = {"type":"AUTH_OK", "driver_id": driver_id, "cp_id": cp_id}
                producer.send(TOPIC_DRIVER, resp)
                producer.flush()
                print(f"[CENTRAL] Authorized driver {driver_id} for {cp_id}")
                # send START_CHARGE command to CP (include driver_id)
                cmd = {"type":"START_CHARGE", "cp_id": cp_id, "driver_id": driver_id, "timestamp": time.time()}
                producer.send(TOPIC_CP_COMMANDS, cmd)
                producer.flush()
            else:
                resp = {"type":"AUTH_DENIED", "driver_id": driver_id, "reason":"CP_faulty"}
                producer.send(TOPIC_DRIVER, resp)
                producer.flush()
                print(f"[CENTRAL] Denied request from {driver_id} for {cp_id}: CP faulty")
        elif mtype == "CANCEL_CHARGE":
            driver_id = data.get("driver_id")
            # broadcast cancel to all CPs or targeted CP if provided
            cp_id = data.get("cp_id")
            cmd = {"type":"CANCEL_CHARGE", "driver_id": driver_id, "cp_id": cp_id, "timestamp": time.time()}
            producer.send(TOPIC_CP_COMMANDS, cmd)
            producer.flush()
            print(f"[CENTRAL] Cancel request from {driver_id} forwarded to CPs")
        else:
            print("[CENTRAL] Unknown driver message type:", mtype)

def handle_cp_messages():
    print("[CENTRAL] CP consumer started.")
    for msg in consumer_cp:
        data = msg.value
        mtype = data.get("type")
        cp_id = data.get("cp_id")
        # Registration handling from monitor or engine
        if mtype == "REGISTER":
            CP_REGISTRY[cp_id] = {"registered":True, "last_seen": time.time(), "status":"ok", "info": data.get("info",{})}
            print(f"[CENTRAL] Registered CP {cp_id}: {CP_REGISTRY[cp_id]}")
        elif mtype == "HEARTBEAT" or mtype == "STATUS":
            entry = CP_REGISTRY.setdefault(cp_id, {"registered":False, "last_seen": None, "status": "unknown", "info":{}})
            entry["last_seen"] = time.time()
            entry["status"] = data.get("status","ok")
            entry["info"] = data.get("info",{})
            # Optionally inform drivers of CP status via broadcast
            print(f"[CENTRAL] Status update from {cp_id}: {entry['status']}")
        elif mtype == "CHARGE_DATA":
            # Telemetry while charging - forward summary to interested parties if needed
            driver_id = data.get("driver_id")
            # send periodic update to driver
            if driver_id:
                update = {"type":"CHARGING_UPDATE", "driver_id": driver_id, "info": {"kwh": data.get("energy"), "cost": data.get("cost")}}
                producer.send(TOPIC_DRIVER, update)
                producer.flush()
        elif mtype == "CHARGE_COMPLETE":
            driver_id = data.get("driver_id")
            ticket = {"cp_id": cp_id, "energy": data.get("energy"), "cost": data.get("cost"), "timestamp": time.time()}
            if driver_id:
                finish = {"type":"FINISHED", "driver_id": driver_id, "ticket": ticket}
                producer.send(TOPIC_DRIVER, finish)
                producer.flush()
                print(f"[CENTRAL] Charge complete for driver {driver_id} at {cp_id}")
        elif mtype == "FAULT":
            entry = CP_REGISTRY.setdefault(cp_id, {"registered":False, "last_seen": None, "status": "unknown", "info":{}})
            entry["status"] = "faulty"
            entry["last_seen"] = time.time()
            print(f"[CENTRAL] Fault reported from {cp_id}")
            # Optionally notify drivers or ops
            broadcast = {"type":"BROADCAST", "message": f"CP {cp_id} reported FAULT"}
            producer.send(TOPIC_DRIVER, broadcast)
            producer.flush()
        else:
            print("[CENTRAL] Unknown CP message type:", mtype)

def main():
    t1 = threading.Thread(target=handle_driver_messages, daemon=True)
    t2 = threading.Thread(target=handle_cp_messages, daemon=True)
    t1.start(); t2.start()
    print("[CENTRAL] EV_Central running. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[CENTRAL] Shutting down.")

if __name__ == '__main__':
    main()
