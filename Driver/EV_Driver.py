"""
EV_Driver_fixed.py - Versión mejorada del Driver para la práctica EVCharging.

Características implementadas:
- Envío real de mensajes JSON al topic de la Central (TOPIC_CENTRAL).
- Procesamiento de mensajes de la Central y filtrado por driver_id.
- Estados correctos: idle, requesting, authorized, charging, finished, error.
- Manejo de fichero de servicios en modo automático.
- Reconexión a Kafka en caso de fallo.
- Uso correcto de main(driver_id) y posibilidad de importar como módulo.
- Mensajería en JSON (puedes adaptar al protocolo STX/ETX si lo necesitas).
- Documentación mínima en los prints para facilitar la evaluación.

IMPORTANTE:
- Necesita instalar 'kafka-python' y disponer del fichero 'config/config.yaml' según tu repo original.
- Para ejecutar: python EV_Driver_fixed.py <id_driver>
"""

import sys
import time
import json
import threading
import yaml
import os
from Extras.utils import resolve_broker
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# Cargar configuración
try:
    with open('config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
except Exception as e:
    print(f"[ERROR] No se pudo leer config/config.yaml: {e}")
    config = {
        'kafka': {
            'broker': 'localhost:9092',
            'topic_central': 'ev_central',
            'topic_driver': 'ev_driver'
        }
    }

# Permite configurar el broker desde env para despliegues en hosts separados.
KAFKA_BROKER = resolve_broker(config, "DRIVER")
TOPIC_CENTRAL = config['kafka'].get('topic_central', 'ev_central')
TOPIC_DRIVER = config['kafka'].get('topic_driver', 'ev_driver')

class Driver:
    def __init__(self, id_driver):
        self.id_driver = str(id_driver)
        self.producer = None
        self.consumer = None
        self.state = 'idle'  # idle, requesting, authorized, charging, finished, error
        self.lock = threading.Lock()
        self.running = True
        self.current_cp = None
        self._stop = threading.Event()
        self._connect_kafka()

    def _connect_kafka(self):
        """Conecta producer y consumer a Kafka con reintentos."""
        while True:
            try:
                print(f"[Driver {self.id_driver}] Conectando a Kafka en {KAFKA_BROKER}...")
                self.producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKER,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )
                # Consumer escuchará mensajes dirigidos a drivers (TOPIC_DRIVER)
                self.consumer = KafkaConsumer(
                    TOPIC_DRIVER,
                    bootstrap_servers=KAFKA_BROKER,
                    group_id=f'driver_{self.id_driver}',
                    auto_offset_reset='earliest',
                    consumer_timeout_ms=1000,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
                )
                print(f"[Driver {self.id_driver}] Conectado a Kafka. Producer y Consumer listos.")
                break
            except (KafkaError, NoBrokersAvailable) as e:
                print(f"[Driver {self.id_driver}] Error conectando a Kafka: {e}. Reintentando en 2s...")
                time.sleep(2)

    def run(self):
        print(f"[Driver {self.id_driver}] Iniciado. Estado inicial: {self.state}")
        # Lanzar thread para escuchar Kafka
        kafka_thread = threading.Thread(target=self.kafka_loop, daemon=True)
        kafka_thread.start()
        # Lanzar heartbeat/hello a CENTRAL
        hb_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        hb_thread.start()
        # Lanzar interfaz de usuario principal
        self.user_interface()

    def kafka_loop(self):
        """Bucle que consume mensajes de TOPIC_DRIVER y los pasa a handle_message."""
        while self.running:
            try:
                for message in self.consumer:
                    # message.value ya es un dict gracias al deserializador
                    try:
                        self.handle_message(message.value)
                    except Exception as ex:
                        print(f"[Driver {self.id_driver}] Error procesando mensaje: {ex}")
                # si el for termina por consumer_timeout_ms se vuelve a iterar
            except (KafkaError, NoBrokersAvailable) as e:
                print(f"[Driver {self.id_driver}] Error Kafka en kafka_loop: {e}. Reconectando...")
                time.sleep(1)
                self._connect_kafka()
            except Exception as e:
                print(f"[Driver {self.id_driver}] Error inesperado en kafka_loop: {e}")
                time.sleep(1)

    def user_interface(self):
        def print_menu():
            print(f"\n{'='*60}")
            print(f"Driver {self.id_driver} - Menú Interactivo (Estado: {self.state})")
            print(f"{'='*60}")
            print("Comandos:")
            print("  1 - Solicitar recarga manual")
            print("  2 - Solicitar recarga desde fichero de servicios")
            print("  3 - Cancelar recarga en curso")
            print("  4 - Mostrar estado actual")
            print("  q - Salir")
            print(f"{'='*60}\n")
        print_menu()
        while self.running:
            try:
                cmd = input(f"[Driver {self.id_driver}] > ").strip().lower()
                if cmd == '1':
                    cp_id = input("Introduce el ID del punto de recarga (CPxx): ").strip().upper()
                    self.solicitar_recarga(cp_id)
                elif cmd == '2':
                    file_path = input("Ruta del fichero de servicios: ").strip()
                    self.solicitar_recarga_fichero(file_path)
                elif cmd == '3':
                    self.cancelar_recarga()
                elif cmd == '4' or cmd == 's':
                    self.mostrar_estado()
                elif cmd == 'q':
                    print("Saliendo...")
                    self.shutdown()
                    self.running = False
                    break
                elif cmd == '':
                    continue
                else:
                    print("Comando no reconocido. Opciones: 1, 2, 3, 4, q")
            except KeyboardInterrupt:
                print("\nInterrupción recibida. Saliendo...")
                self.shutdown()
                self.running = False
                break

    def heartbeat_loop(self):
        """Envía un HELLO/heartbeat periódico a CENTRAL para visibilidad."""
        while not self._stop.is_set():
            msg = {
                "type": "DRIVER_HELLO",
                "driver_id": self.id_driver,
                "state": self.state,
                "cp_id": self.current_cp,
                "timestamp": time.time(),
            }
            try:
                self.producer.send(TOPIC_CENTRAL, value=msg)
                self.producer.flush(timeout=2)
            except Exception:
                pass
            time.sleep(15)

    def solicitar_recarga(self, cp_id):
        """Envía solicitud de recarga a CENTRAL por Kafka."""
        with self.lock:
            if self.state in ('requesting', 'authorized', 'charging'):
                print(f"[Driver {self.id_driver}] No se puede solicitar: estado actual = {self.state}")
                return
            self.state = 'requesting'
            self.current_cp = cp_id
        msg = {
            "type": "REQUEST_CHARGE",
            "driver_id": self.id_driver,
            "cp_id": cp_id,
            "timestamp": time.time()
        }
        try:
            fut = self.producer.send(TOPIC_CENTRAL, value=msg)
            self.producer.flush(timeout=5)
            print(f"[Driver {self.id_driver}] Petición enviada a CENTRAL para {cp_id}.")
        except KafkaError as e:
            print(f"[Driver {self.id_driver}] Error enviando petición a CENTRAL: {e}")
            with self.lock:
                self.state = 'error'

    def solicitar_recarga_fichero(self, file_path):
        print(f"[Driver {self.id_driver}] Procesando servicios desde fichero: {file_path}")
        try:
            with open(file_path, 'r') as f:
                for line in f:
                    if not self.running:
                        break
                    cp_id = line.strip().split()[0]
                    if not cp_id:
                        continue
                    self.solicitar_recarga(cp_id)
                    # Esperar a que termine o 10s máximos; la práctica pide 4s entre servicios,
                    # pero esperaremos a que el ciclo de autorización/finalización ocurra.
                    wait_seconds = 0
                    while self.running and wait_seconds < 30 and self.state not in ('idle','error'):
                        time.sleep(1)
                        wait_seconds += 1
                    # Después de finalizada (o timeout), esperar 4 segundos antes del siguiente
                    time.sleep(4)
        except Exception as e:
            print(f"[Driver {self.id_driver}] Error leyendo fichero: {e}")

    def cancelar_recarga(self):
        """Solicita cancelación local y notifica a CENTRAL si procede."""
        with self.lock:
            if self.state not in ('requesting', 'authorized', 'charging'):
                print(f"[Driver {self.id_driver}] No hay recarga en curso para cancelar (estado={self.state}).")
                return
            prev_state = self.state
            self.state = 'idle'
            target_cp = self.current_cp
            self.current_cp = None
        print(f"[Driver {self.id_driver}] Recarga cancelada (antes: {prev_state}).")
        # Notificar a CENTRAL (opcional pero útil para sincronizar)
        msg = {
            "type": "CANCEL_CHARGE",
            "driver_id": self.id_driver,
            "cp_id": target_cp,
            "timestamp": time.time()
        }
        try:
            self.producer.send(TOPIC_CENTRAL, value=msg)
            self.producer.flush(timeout=5)
        except KafkaError as e:
            print(f"[Driver {self.id_driver}] Error notificando cancelación a CENTRAL: {e}")

    def mostrar_estado(self):
        print(f"[Driver {self.id_driver}] Estado actual: {self.state}")

    def handle_message(self, data):
        """
        Procesa mensajes entrantes desde CENTRAL (TOPIC_DRIVER).
        Espera mensajes JSON con al menos: type, driver_id.
        Filtra mensajes que no estén dirigidos a este driver.
        """
        if not isinstance(data, dict):
            print(f"[Driver {self.id_driver}] Mensaje inválido (no es JSON dict): {data}")
            return

        # Verificar driver_id en el mensaje (si lo incluye)
        target = data.get('driver_id')
        if target is None:
            # Mensajes broadcast también pueden procesarse dependiendo del tipo
            pass
        elif str(target) != self.id_driver:
            # Mensaje para otro conductor -> ignorar
            return

        mtype = data.get('type', '').upper()
        if mtype == 'AUTH_OK' or mtype == 'AUTHORIZED':
            with self.lock:
                self.state = 'authorized'
                self.current_cp = data.get('cp_id', self.current_cp)
            print(f"[Driver {self.id_driver}] Autorizado por CENTRAL. Conectar vehículo para iniciar suministro.")
        elif mtype == 'AUTH_DENIED' or mtype == 'DENIED':
            with self.lock:
                self.state = 'idle'
                self.current_cp = None
            reason = data.get('reason', 'sin motivo dado')
            print(f"[Driver {self.id_driver}] Autorización denegada: {reason}")
        elif mtype == 'START_CHARGING' or mtype == 'CHARGING_STARTED':
            with self.lock:
                self.state = 'charging'
                self.current_cp = data.get('cp_id', self.current_cp)
            print(f"[Driver {self.id_driver}] Inicio de suministro. Datos iniciales: {data.get('info', {})}")
        elif mtype == 'CHARGING_UPDATE':
            # Mensajes periódicos durante la carga: kwh, cost
            info = data.get('info', {})
            kwh = info.get('kwh')
            cost = info.get('cost')
            print(f"[Driver {self.id_driver}] Actualización: {kwh} kWh, {cost} €")
            with self.lock:
                self.state = 'charging'
        elif mtype == 'FINISHED' or mtype == 'CHARGING_FINISHED':
            ticket = data.get('ticket', {})
            print(f"[Driver {self.id_driver}] Recarga finalizada. Ticket: {ticket}")
            with self.lock:
                self.state = 'idle'
                self.current_cp = None
        elif mtype == 'CP_ERROR' or mtype == 'ERROR':
            err = data.get('error', 'error desconocido')
            print(f"[Driver {self.id_driver}] Error en CP/CENTRAL: {err}")
            with self.lock:
                self.state = 'error'
                self.current_cp = None
        elif mtype == 'BROADCAST':
            # Mensajes informativos globales
            text = data.get('message', '')
            print(f"[Driver {self.id_driver}] Broadcast: {text}")
        else:
            print(f"[Driver {self.id_driver}] Mensaje desconocido recibido: {data}")

    def shutdown(self):
        """Cancela carga activa antes de salir."""
        with self.lock:
            active = self.state in ('requesting', 'authorized', 'charging')
            cp_target = self.current_cp
        if active:
            msg = {
                "type": "CANCEL_CHARGE",
                "driver_id": self.id_driver,
                "cp_id": cp_target,
                "timestamp": time.time()
            }
            try:
                self.producer.send(TOPIC_CENTRAL, value=msg)
                self.producer.flush(timeout=2)
            except Exception:
                pass
        self._stop.set()

def main(driver_id=None):
    # Permitir llamar main() con argumento o desde CLI
    if driver_id is None:
        if len(sys.argv) < 2:
            print('Uso: python EV_Driver_fixed.py <id_driver>')
            sys.exit(1)
        driver_id = sys.argv[1]
    d = Driver(driver_id)
    d.run()

if __name__ == '__main__':
    main()
