"""
EV_Central.py - Central con gestión básica (Kafka + persistencia SQLite)
"""

import json
import time
import threading
import yaml
import os
import sqlite3
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

with open("config/config.yaml","r") as f:
    config = yaml.safe_load(f)

BROKER = resolve_broker(config, "CENTRAL")
DB_PATH = os.getenv("CENTRAL_DB", "Central/bdd/evcharging.db")
TOPIC_CENTRAL = config["kafka"]["topic_central"]      # driver -> central
TOPIC_DRIVER = config["kafka"]["topic_driver"]        # central -> driver
TOPIC_CP_COMMANDS = config["kafka"]["topic_cp_commands"]  # central -> cp
TOPIC_CP_DATA = config["kafka"]["topic_cp_data"]      # cp -> central (telemetria)
TOPIC_CP_STATUS = config["kafka"]["topic_cp_status"]  # cp -> monitor/central (estado)

producer = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda v: json.dumps(v).encode("utf-8"))
consumer_driver = KafkaConsumer(TOPIC_CENTRAL, bootstrap_servers=BROKER, value_deserializer=lambda v: json.loads(v.decode("utf-8")), auto_offset_reset="earliest", group_id="central_drivers")
consumer_cp = KafkaConsumer(TOPIC_CP_DATA, TOPIC_CP_STATUS, bootstrap_servers=BROKER, value_deserializer=lambda v: json.loads(v.decode("utf-8")), auto_offset_reset="earliest", group_id="central_cps")

db_lock = threading.Lock()
conn = sqlite3.connect(DB_PATH, check_same_thread=False)
conn.row_factory = sqlite3.Row

# Registro de CPs: cp_id -> info dict {registrado: bool, last_seen: ts, status: str}
CP_REGISTRY = {}
# Sesiones activas: (cp_id, driver_id) -> session_id
ACTIVE_SESSIONS = {}
STALE_THRESHOLD = 15  # segundos sin heartbeat -> desconectado
drivers_state = {}  # driver_id -> {state, cp_id, last_update}
DRIVER_STALE = 300   # segundos sin novedades -> se oculta del snapshot

def set_driver_state(driver_id, state, cp_id=None):
    drivers_state[driver_id] = {"state": state, "cp_id": cp_id, "last_update": time.time()}

def init_db():
    with db_lock:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS charging_points (
                cp_id TEXT PRIMARY KEY,
                state TEXT,
                last_update TIMESTAMP,
                total_charges INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS charging_sessions (
                session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                cp_id TEXT,
                driver_id TEXT,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                energy_kwh REAL,
                cost REAL,
                state TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP,
                cp_id TEXT,
                event_type TEXT,
                description TEXT
            )
        """)
        conn.commit()
        # Pre-cargar registros previos
        for row in conn.execute("SELECT cp_id, state, last_update FROM charging_points"):
            last_seen = None
            if row["last_update"]:
                try:
                    last_seen = datetime.fromisoformat(row["last_update"]).timestamp()
                except Exception:
                    last_seen = time.time()
            CP_REGISTRY[row["cp_id"]] = {"registered": True, "last_seen": last_seen, "status": row["state"], "info": {}}

def now_ts():
    return datetime.now(timezone.utc).isoformat()

def log_event(cp_id, event_type, description):
    with db_lock:
        conn.execute("INSERT INTO events(timestamp, cp_id, event_type, description) VALUES(?,?,?,?)",
                     (now_ts(), cp_id, event_type, description))
        conn.commit()

def upsert_cp(cp_id, state):
    ts = now_ts()
    with db_lock:
        cur = conn.execute("SELECT cp_id FROM charging_points WHERE cp_id=?", (cp_id,))
        if cur.fetchone():
            conn.execute("UPDATE charging_points SET state=?, last_update=? WHERE cp_id=?", (state, ts, cp_id))
        else:
            conn.execute("INSERT INTO charging_points(cp_id, state, last_update, total_charges) VALUES(?,?,?,0)", (cp_id, state, ts))
        conn.commit()
    CP_REGISTRY[cp_id] = {"registered": True, "last_seen": time.time(), "status": state, "info": {}}

def start_session(cp_id, driver_id):
    with db_lock:
        cur = conn.execute("INSERT INTO charging_sessions(cp_id, driver_id, start_time, state) VALUES(?,?,?,?)",
                           (cp_id, driver_id, now_ts(), "running"))
        conn.commit()
        return cur.lastrowid

def update_session(session_id, energy=None, cost=None, end=False, cancelled=False, fault=False):
    with db_lock:
        fields = []
        params = []
        if energy is not None:
            fields.append("energy_kwh=?"); params.append(energy)
        if cost is not None:
            fields.append("cost=?"); params.append(cost)
        if end:
            fields.append("end_time=?"); params.append(now_ts())
            if cancelled:
                fields.append("state=?"); params.append("cancelled")
            elif fault:
                fields.append("state=?"); params.append("fault")
            else:
                fields.append("state=?"); params.append("finished")
        set_clause = ", ".join(fields)
        params.append(session_id)
        conn.execute(f"UPDATE charging_sessions SET {set_clause} WHERE session_id=?", params)
        conn.commit()

def handle_driver_messages():
    print("[CENTRAL] Driver consumer funcionando.")
    for msg in consumer_driver:
        data = msg.value
        mtype = data.get("type")
        if mtype == "REQUEST_CHARGE":
            driver_id = data.get("driver_id")
            cp_id = data.get("cp_id")
            set_driver_state(driver_id, "requesting", cp_id)
            cp_info = CP_REGISTRY.get(cp_id)
            now = time.time()
            if not cp_info or not cp_info.get("registered", False):
                resp = {"type":"AUTH_DENIED", "driver_id": driver_id, "reason":"CP_not_registered"}
                producer.send(TOPIC_DRIVER, resp); producer.flush()
                set_driver_state(driver_id, "idle", None)
                continue
            # Desconectado por falta de latidos
            if cp_info.get("last_seen") and now - cp_info["last_seen"] > STALE_THRESHOLD:
                resp = {"type":"AUTH_DENIED", "driver_id": driver_id, "reason":"CP_disconnected"}
                producer.send(TOPIC_DRIVER, resp); producer.flush()
                set_driver_state(driver_id, "idle", None)
                continue
            if cp_info.get("status") == "ok":
                resp = {"type":"AUTH_OK", "driver_id": driver_id, "cp_id": cp_id}
                producer.send(TOPIC_DRIVER, resp); producer.flush()
                set_driver_state(driver_id, "authorized", cp_id)
                session_id = ACTIVE_SESSIONS.get((cp_id, driver_id))
                if not session_id:
                    session_id = start_session(cp_id, driver_id)
                    ACTIVE_SESSIONS[(cp_id, driver_id)] = session_id
                cmd = {"type":"START_CHARGE", "cp_id": cp_id, "driver_id": driver_id, "timestamp": time.time()}
                producer.send(TOPIC_CP_COMMANDS, cmd); producer.flush()
                log_event(cp_id, "AUTH_OK", f"Driver {driver_id} autorizado")
            else:
                resp = {"type":"AUTH_DENIED", "driver_id": driver_id, "reason":"CP_faulty_or_unavailable"}
                producer.send(TOPIC_DRIVER, resp); producer.flush()
            set_driver_state(driver_id, "idle", None)
        elif mtype == "CANCEL_CHARGE":
            driver_id = data.get("driver_id")
            cp_id = data.get("cp_id")
            cmd = {"type":"CANCEL_CHARGE", "driver_id": driver_id, "cp_id": cp_id, "timestamp": time.time()}
            producer.send(TOPIC_CP_COMMANDS, cmd); producer.flush()
            log_event(cp_id or "ALL", "CANCEL", f"Cancel solicitado por {driver_id}")
            set_driver_state(driver_id, "idle", None)
        elif mtype == "DRIVER_HELLO":
            driver_id = data.get("driver_id")
            cp_id = data.get("cp_id")
            state = data.get("state","idle")
            set_driver_state(driver_id, state, cp_id)

def handle_cp_messages():
    print("[CENTRAL] CP consumer started.")
    for msg in consumer_cp:
        data = msg.value
        mtype = data.get("type")
        cp_id = data.get("cp_id")
        if mtype == "REGISTER":
            upsert_cp(cp_id, "ok")
            log_event(cp_id, "REGISTER", "CP registrado")
        elif mtype in ("HEARTBEAT", "STATUS"):
            st = data.get("status","ok")
            entry = CP_REGISTRY.setdefault(cp_id, {"registered":False, "last_seen": None, "status": "unknown", "info":{}})
            entry["last_seen"] = time.time()
            # Solo actualizamos a faulty si viene un FAULT; los heartbeats con faulty se ignoran.
            if st != "faulty":
                upsert_cp(cp_id, st)
        elif mtype == "CHARGE_DATA":
            driver_id = data.get("driver_id")
            session_id = ACTIVE_SESSIONS.get((cp_id, driver_id))
            if not session_id:
                session_id = start_session(cp_id, driver_id)
                ACTIVE_SESSIONS[(cp_id, driver_id)] = session_id
            update_session(session_id, energy=data.get("energy"), cost=data.get("cost"))
            if driver_id:
                set_driver_state(driver_id, "charging", cp_id)
                update = {"type":"CHARGING_UPDATE", "driver_id": driver_id, "info": {"kwh": data.get("energy"), "cost": data.get("cost")}}
                producer.send(TOPIC_DRIVER, update); producer.flush()
        elif mtype == "CHARGE_COMPLETE":
            driver_id = data.get("driver_id")
            session_id = ACTIVE_SESSIONS.pop((cp_id, driver_id), None)
            update_session(session_id, energy=data.get("energy"), cost=data.get("cost"), end=True, cancelled=data.get("cancelled", False)) if session_id else None
            ticket = {"cp_id": cp_id, "energy": data.get("energy"), "cost": data.get("cost"), "timestamp": time.time()}
            if driver_id:
                finish = {"type":"FINISHED", "driver_id": driver_id, "ticket": ticket}
                producer.send(TOPIC_DRIVER, finish); producer.flush()
                set_driver_state(driver_id, "idle", None)
            log_event(cp_id, "CHARGE_COMPLETE", f"Driver {driver_id} energía {data.get('energy')}")
        elif mtype == "FAULT":
            upsert_cp(cp_id, "faulty")
            log_event(cp_id, "FAULT", "Reporte de avería")
            broadcast = {"type":"BROADCAST", "message": f"CP {cp_id} reported FAULT"}
            producer.send(TOPIC_DRIVER, broadcast); producer.flush()
            # Abort sesiones activas
            for (cp, drv), sid in list(ACTIVE_SESSIONS.items()):
                if cp == cp_id:
                    update_session(sid, end=True, fault=True)
                    ACTIVE_SESSIONS.pop((cp, drv), None)
                    err = {"type":"CP_ERROR", "driver_id": drv, "error": "CP_fault"}
                    producer.send(TOPIC_DRIVER, err); producer.flush()
                    drivers_state[drv] = {"state":"error", "cp_id": None}
        else:
            log_event(cp_id, "UNKNOWN", f"Tipo desconocido {mtype}")

def admin_console():
    def print_menu():
        print("\n" + "=" * 60)
        print("Central - Menú admin")
        print("=" * 60)
        print("Comandos:")
        print("  list                     - Listar CPs conocidos")
        print("  list-drivers             - Listar drivers activos")
        print("  pause <cp|all>           - Pausar carga(s)")
        print("  resume <cp|all>          - Reanudar carga(s)")
        print("  stop <cp|all>            - Detener carga(s)")
        print("  q                        - Salir")
        print("=" * 60)
    print_menu()
    while True:
        try:
            cmd = input("[CENTRAL] > ").strip().lower()
        except EOFError:
            break
        if not cmd:
            continue
        parts = cmd.split()
        action = parts[0]
        if action == "list":
            for cp_id, info in CP_REGISTRY.items():
                last_seen = info.get("last_seen")
                if isinstance(last_seen, str):
                    try:
                        last_seen = datetime.fromisoformat(last_seen).timestamp()
                    except Exception:
                        last_seen = time.time()
                if last_seen is None:
                    delta_txt = "never"
                else:
                    delta_txt = f"{time.time()-last_seen:.1f}s ago"
                print(f"{cp_id}: {info.get('status','?')} (seen {delta_txt})")
        elif action == "list-drivers":
            now = time.time()
            if not drivers_state:
                print("Sin drivers activos")
            for drv, info in drivers_state.items():
                lu = info.get("last_update")
                delta = f"{now-lu:.1f}s ago" if lu else "n/a"
                print(f"{drv}: state={info.get('state')} cp={info.get('cp_id')} last_upd={delta}")
        elif action in ("pause","resume","stop"):
            target = None if len(parts)==1 or parts[1]=="all" else parts[1].upper()
            mtype = {"pause":"PAUSE_CHARGE","resume":"RESUME_CHARGE","stop":"STOP_CHARGE"}[action]
            payload = {"type": mtype, "cp_id": target, "timestamp": time.time()}
            producer.send(TOPIC_CP_COMMANDS, payload); producer.flush()
            log_event(target or "ALL", mtype, "Enviado desde consola")
            print(f"Enviado {mtype} a {target or 'ALL'}")
        elif action in ("q","quit","exit"):
            break
        else:
            print("Comando no reconocido.")
        print_menu()

# -----------------------------------------------------------
# API WEB (FastAPI)
# -----------------------------------------------------------

app = FastAPI(title="EVCharging Central API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def snapshot_data():
    cps = []
    now = time.time()
    # map cp -> driver activo
    active_by_cp = {}
    for (cp_id, drv_id), sid in ACTIVE_SESSIONS.items():
        active_by_cp[cp_id] = drv_id
    active_sessions = []
    with db_lock:
        for (cp_id, drv_id), sid in ACTIVE_SESSIONS.items():
            row = conn.execute("SELECT session_id, cp_id, driver_id, energy_kwh, cost, state FROM charging_sessions WHERE session_id=?", (sid,)).fetchone()
            if row:
                active_sessions.append(dict(row))
    for cp_id, info in CP_REGISTRY.items():
        status_raw = info.get("status","unknown")
        last_seen = info.get("last_seen")
        stale = last_seen and now - last_seen > STALE_THRESHOLD
        status = "DESCONECTADO"
        if status_raw == "faulty":
            status = "AVERIA"
        elif not stale:
            status = "CONECTADO"
            if cp_id in active_by_cp:
                status = "CARGANDO"
        cps.append({
            "cp_id": cp_id,
            "status": status,
            "last_seen": last_seen,
            "last_seen_delta": (now - last_seen) if last_seen else None,
            "driver_id": active_by_cp.get(cp_id),
        })
    sessions = []
    with db_lock:
        for row in conn.execute("SELECT session_id, cp_id, driver_id, energy_kwh, cost, state FROM charging_sessions ORDER BY session_id DESC LIMIT 50"):
            sessions.append(dict(row))
    drivers = []
    # Usa estado en memoria, y si está vacío, recurre a sesiones recientes.
    # Solo mostramos drivers activos con updates recientes (no persistidos).
    stale_cutoff = now - DRIVER_STALE
    to_delete = []
    for drv, info in drivers_state.items():
        lu = info.get("last_update", 0)
        if lu < stale_cutoff:
            to_delete.append(drv)
            continue
        d_state = info.get("state","idle")
        cp_target = info.get("cp_id")
        state_label = "DESCONECTADO"
        if d_state == "charging":
            state_label = "CARGANDO"
        elif d_state in ("authorized","requesting","idle","error"):
            state_label = "CONECTADO"
        drivers.append({"driver_id": drv, "state": state_label, "cp_id": cp_target})
    # Limpia entradas viejas para no arrastrar ruido
    for drv in to_delete:
        drivers_state.pop(drv, None)
    return {"cps": cps, "drivers": drivers, "sessions": sessions, "active_sessions": active_sessions}

@app.get("/snapshot")
def snapshot():
    return snapshot_data()

@app.post("/command")
def command(payload: dict):
    action = payload.get("action")
    cp_id = payload.get("cp_id")
    if action not in ("pause","resume","stop","cancel","start"):
        return {"status": "error", "message": "acción no soportada"}
    target = None if payload.get("scope") == "all" else cp_id
    if action in ("pause","resume","stop"):
        mtype = {"pause":"PAUSE_CHARGE","resume":"RESUME_CHARGE","stop":"STOP_CHARGE"}[action]
        cmd = {"type": mtype, "cp_id": target, "timestamp": time.time()}
        producer.send(TOPIC_CP_COMMANDS, cmd); producer.flush()
        log_event(target or "ALL", mtype, "Enviado via API")
        return {"status": "ok"}
    if action == "cancel":
        driver_id = payload.get("driver_id")
        cmd = {"type":"CANCEL_CHARGE","driver_id": driver_id, "cp_id": target, "timestamp": time.time()}
        producer.send(TOPIC_CP_COMMANDS, cmd); producer.flush()
        log_event(target or "ALL", "CANCEL", "Enviado via API")
        if driver_id:
            drivers_state[driver_id] = {"state":"idle", "cp_id": None}
        return {"status":"ok"}
    if action == "start":
        driver_id = payload.get("driver_id")
        if not driver_id or not cp_id:
            return {"status":"error", "message":"driver_id y cp_id requeridos"}
        cp_info = CP_REGISTRY.get(cp_id)
        now = time.time()
        if not cp_info or not cp_info.get("registered", False):
            return {"status":"error", "message":"CP no registrado"}
        if cp_info.get("last_seen") and now - cp_info["last_seen"] > STALE_THRESHOLD:
            return {"status":"error", "message":"CP desconectado"}
        if cp_info.get("status") != "ok":
            return {"status":"error", "message":"CP no disponible"}
        # inicia sesión
        session_id = ACTIVE_SESSIONS.get((cp_id, driver_id))
        if not session_id:
            session_id = start_session(cp_id, driver_id)
            ACTIVE_SESSIONS[(cp_id, driver_id)] = session_id
        drivers_state[driver_id] = {"state":"authorized", "cp_id": cp_id}
        cmd = {"type":"START_CHARGE", "cp_id": cp_id, "driver_id": driver_id, "timestamp": time.time()}
        producer.send(TOPIC_CP_COMMANDS, cmd); producer.flush()
        log_event(cp_id, "AUTH_OK", f"Driver {driver_id} autorizado vía API")
        return {"status":"ok"}
    return {"status": "error", "message": "acción no soportada"}

def start_api():
    def run():
        uvicorn.run(app, host="0.0.0.0", port=8000, log_level="warning")
    threading.Thread(target=run, daemon=True).start()

def main():
    init_db()
    t1 = threading.Thread(target=handle_driver_messages, daemon=True)
    t2 = threading.Thread(target=handle_cp_messages, daemon=True)
    t3 = threading.Thread(target=admin_console, daemon=True)
    t1.start(); t2.start(); t3.start()
    start_api()
    print("[CENTRAL] EV_Central running. Ctrl+C para salir.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("[CENTRAL] Shutting down.")

if __name__ == '__main__':
    main()
