import os
import time
import yaml
import logging
from functools import reduce
from operator import xor
from kafka import KafkaProducer
from kafka.errors import KafkaError


def robust_kafka_connect(connect_func, name, retry_interval=2):
    """
    Intenta conectar a Kafka reintentando hasta exito.
    connect_func: funcion que realiza la conexion y retorna el objeto conectado.
    name: nombre del componente para logs.
    retry_interval: segundos entre reintentos.
    """
    while True:
        try:
            obj = connect_func()
            print(f'[{name}] Conectado a Kafka.')
            return obj
        except KafkaError as e:
            print(f'[{name}] Error conectando a Kafka: {e}. Reintentando en {retry_interval}s...')
            time.sleep(retry_interval)


def kafka_reconnect_loop(connect_func, name, on_error=None, retry_interval=2):
    """
    Bucle de reconexion robusta para consumidores/producers Kafka.
    connect_func: funcion que realiza la conexion y retorna el objeto conectado.
    name: nombre del componente para logs.
    on_error: funcion opcional a ejecutar en error antes de reintentar.
    retry_interval: segundos entre reintentos.
    """
    while True:
        try:
            return connect_func()
        except KafkaError as e:
            print(f'[{name}] Error de Kafka: {e}. Reintentando en {retry_interval}s...')
            if on_error:
                on_error()
            time.sleep(retry_interval)


def resolve_broker(config, component_name="APP"):
    """
    Devuelve el broker Kafka accesible. Usa KAFKA_BROKER si esta definida,
    si no, el valor de config.yaml. Si no es accesible, intenta autodeteccion
    (busca broker en la red local) y avisa para evitar que cada maquina use su
    propio localhost.
    """
    env_broker = os.getenv("KAFKA_BROKER")
    broker = env_broker or config["kafka"].get("broker")
    try:
        tmp = KafkaProducer(
            bootstrap_servers=broker,
            request_timeout_ms=2000,
            api_version_auto_timeout_ms=2000,
        )
        tmp.close()
        return broker
    except Exception:
        try:
            from Extras.kafka_discovery import detect_kafka_broker
            detected = detect_kafka_broker()
        except Exception:
            detected = None
        if detected:
            print(f"[{component_name}] Broker {broker} no disponible. Usando {detected}.")
            return detected
        print(f"[{component_name}] No se pudo contactar con Kafka en {broker}. "
              f"Define KAFKA_BROKER con la IP:PUERTO del broker compartido.")
        return broker


def load_config():
    """Carga la configuracion desde el archivo config.yaml"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def calculate_lrc(message):
    """Calcula el LRC (Longitudinal Redundancy Check) de un mensaje mediante XOR de todos los bytes."""
    if isinstance(message, str):
        message = message.encode('utf-8')
    return reduce(xor, message)


def build_message(code, *fields):
    """Construye un mensaje con el formato <STX><CODIGO>#<CAMPO1>#...#<CAMPON><ETX><LRC>"""
    config = load_config()
    stx = config['protocol']['stx']
    etx = config['protocol']['etx']
    parts = [code] + list(fields)
    message_body = '#'.join(str(p) for p in parts)
    message = f"{stx}{message_body}{etx}"
    lrc = calculate_lrc(message)
    return message + chr(lrc)


def parse_message(message):
    """
    Parsea un mensaje recibido y verifica su integridad.
    Retorna (valido, codigo, campos) o (False, None, None)
    """
    try:
        config = load_config()
        stx = config['protocol']['stx']
        etx = config['protocol']['etx']
        if len(message) < 4:
            return False, None, None
        lrc_received = ord(message[-1])
        message_without_lrc = message[:-1]
        lrc_calculated = calculate_lrc(message_without_lrc)
        if lrc_received != lrc_calculated:
            logging.warning(f"LRC incorrecto. Recibido: {lrc_received}, Calculado: {lrc_calculated}")
            return False, None, None
        if not message_without_lrc.startswith(stx) or not message_without_lrc.endswith(etx):
            return False, None, None
        content = message_without_lrc[len(stx):-len(etx)]
        parts = content.split('#')
        if len(parts) < 1:
            return False, None, None
        code = parts[0]
        fields = parts[1:] if len(parts) > 1 else []
        return True, code, fields
    except Exception as e:
        logging.error(f"Error parseando mensaje: {e}")
        return False, None, None


def get_ack():
    """Retorna el mensaje ACK con formato completo"""
    return build_message('ACK')


def get_nack():
    """Retorna el mensaje NACK con formato completo"""
    return build_message('NAK')


def setup_logging(module_name):
    """Configura el sistema de logging para un modulo"""
    config = load_config()
    logging.basicConfig(
        level=getattr(logging, config['logging']['level']),
        format=config['logging']['format']
    )
    return logging.getLogger(module_name)


def get_color_for_state(state):
    """Retorna el codigo de color ANSI para un estado dado"""
    colors = {
        'Activado': '\033[92m',      # Verde
        'Cargando': '\033[94m',      # Azul
        'Suministrando': '\033[94m', # Azul (compatibilidad)
        'Parado': '\033[93m',        # Amarillo
        'Averiado': '\033[91m',      # Rojo
        'Desconectado': '\033[90m',  # Gris
    }
    reset = '\033[0m'
    return colors.get(state, '') + state + reset
