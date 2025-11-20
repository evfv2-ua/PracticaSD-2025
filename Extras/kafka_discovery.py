"""
Módulo de autodetección de Kafka en la red local
Permite detectar automáticamente el broker Kafka sin necesidad de configuración manual
"""

import socket
import logging
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import time

# Silenciar logs de Kafka
logging.getLogger('kafka').setLevel(logging.ERROR)
logging.getLogger('kafka.conn').setLevel(logging.CRITICAL)
logging.getLogger('kafka.coordinator').setLevel(logging.CRITICAL)
logging.getLogger('kafka.cluster').setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)


def detect_kafka_broker(possible_hosts=None, port=9092, timeout=2):
    """
    Detecta el broker Kafka en la red
    
    Args:
        possible_hosts: Lista de hosts a probar. Si es None, usa localhost y algunas IPs comunes
        port: Puerto de Kafka (por defecto 9092)
        timeout: Timeout para cada intento de conexión
    
    Returns:
        String con el host:port del broker encontrado, o None si no se encuentra
    """
    if possible_hosts is None:
        # Intentar con localhost y algunas IPs comunes en redes de aula
        possible_hosts = [
            'localhost',
            '127.0.0.1',
            'kafka',  # Nombre común en Docker
            socket.gethostbyname(socket.gethostname()),  # IP local
        ]
        
        # Añadir rango de IPs locales común en aulas (192.168.1.x)
        local_ip = socket.gethostbyname(socket.gethostname())
        if local_ip.startswith('192.168.'):
            base = '.'.join(local_ip.split('.')[:3])
            for i in range(1, 255):
                possible_hosts.append(f"{base}.{i}")
    
    for host in possible_hosts:
        broker = f"{host}:{port}"
        try:
            # Intentar crear un productor de prueba
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                request_timeout_ms=timeout * 1000,
                api_version_auto_timeout_ms=timeout * 1000
            )
            producer.close()
            return broker
        except NoBrokersAvailable:
            continue
        except Exception as e:
            logger.debug(f"Error probando {broker}: {e}")
            continue
    
    logger.warning("No se pudo encontrar ningún broker Kafka")
    return None


def wait_for_kafka(broker, max_retries=10, retry_interval=5):
    """
    Espera a que Kafka esté disponible
    
    Args:
        broker: String con host:port del broker
        max_retries: Número máximo de reintentos
        retry_interval: Segundos entre reintentos
    
    Returns:
        True si Kafka está disponible, False en caso contrario
    """
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[broker],
                request_timeout_ms=5000
            )
            producer.close()
            return True
        except Exception as e:
            logger.debug(f"Intento {attempt + 1}/{max_retries} fallido: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_interval)
    
    return False


def get_kafka_connection(config):
    """
    Obtiene la configuración de conexión a Kafka, con autodetección si es necesario
    
    Args:
        config: Diccionario de configuración cargado desde config.yaml
    
    Returns:
        String con el broker (host:port)
    """
    broker = config['kafka']['bootstrap_servers']
    
    # Si el broker configurado no está disponible, intentar autodetección
    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            request_timeout_ms=3000
        )
        producer.close()
        return broker
    except:
        detected_broker = detect_kafka_broker()
        if detected_broker:
            return detected_broker
        else:
            logger.error("No se pudo conectar a ningún broker Kafka")
            return broker  # Retornar el configurado como fallback
