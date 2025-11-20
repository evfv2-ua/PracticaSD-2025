import time
from kafka.errors import KafkaError
def robust_kafka_connect(connect_func, name, retry_interval=2):
    """
    Intenta conectar a Kafka de forma robusta, reintentando hasta éxito.
    connect_func: función que realiza la conexión y retorna el objeto conectado.
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
    Bucle de reconexión robusta para consumidores/producers Kafka.
    connect_func: función que realiza la conexión y retorna el objeto conectado.
    name: nombre del componente para logs.
    on_error: función opcional a ejecutar en error antes de reintentar.
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
"""
Módulo de utilidades comunes para el sistema EVCharging
"""

import yaml
import os
import logging
from functools import reduce
from operator import xor


def load_config():
    """
    Carga la configuración desde el archivo config.yaml
    """
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.yaml')
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def calculate_lrc(message):
    """
    Calcula el LRC (Longitudinal Redundancy Check) de un mensaje
    mediante XOR de todos los bytes
    """
    if isinstance(message, str):
        message = message.encode('utf-8')
    return reduce(xor, message)


def build_message(code, *fields):
    """
    Construye un mensaje con el formato <STX><CÓDIGO>#<CAMPO1>#...#<CAMPON><ETX><LRC>
    """
    config = load_config()
    stx = config['protocol']['stx']
    etx = config['protocol']['etx']
    
    # Construir el mensaje sin LRC
    parts = [code] + list(fields)
    message_body = '#'.join(str(p) for p in parts)
    message = f"{stx}{message_body}{etx}"
    
    # Calcular LRC
    lrc = calculate_lrc(message)
    
    # Mensaje completo
    full_message = message + chr(lrc)
    return full_message


def parse_message(message):
    """
    Parsea un mensaje recibido y verifica su integridad
    Retorna (válido, código, campos) o (False, None, None)
    """
    try:
        config = load_config()
        stx = config['protocol']['stx']
        etx = config['protocol']['etx']
        
        # Separar mensaje y LRC
        if len(message) < 4:
            return False, None, None
        
        lrc_received = ord(message[-1])
        message_without_lrc = message[:-1]
        
        # Verificar LRC
        lrc_calculated = calculate_lrc(message_without_lrc)
        if lrc_received != lrc_calculated:
            logging.warning(f"LRC incorrecto. Recibido: {lrc_received}, Calculado: {lrc_calculated}")
            return False, None, None
        
        # Extraer contenido
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
    """
    Retorna el mensaje ACK con formato completo
    """
    return build_message('ACK')


def get_nack():
    """
    Retorna el mensaje NACK con formato completo
    """
    return build_message('NAK')


def setup_logging(module_name):
    """
    Configura el sistema de logging para un módulo
    """
    config = load_config()
    logging.basicConfig(
        level=getattr(logging, config['logging']['level']),
        format=config['logging']['format']
    )
    return logging.getLogger(module_name)


def get_color_for_state(state):
    """
    Retorna el código de color ANSI para un estado dado
    """
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
