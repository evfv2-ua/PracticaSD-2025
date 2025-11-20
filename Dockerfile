# Dockerfile multietapa para el sistema EVCharging
# Permite generar imágenes para cualquier módulo usando el argumento MODULE
#
# Ejemplos de construcción:
#   docker build --build-arg MODULE=central -t evcharging-central .
#   docker build --build-arg MODULE=cp_engine -t evcharging-cp-engine .
#   docker build --build-arg MODULE=cp_monitor -t evcharging-cp-monitor .
#   docker build --build-arg MODULE=driver -t evcharging-driver .

FROM python:3.11-slim as base

# Configurar entorno
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Instalar dependencias del sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Crear directorio de trabajo
WORKDIR /app

# Copiar requirements y instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el código fuente
COPY . .

# Crear directorios necesarios
RUN mkdir -p Central/bdd Driver Cargador config Extras

# Hacer ejecutable run.py
RUN chmod +x run.py

# Argumento para especificar qué módulo ejecutar
ARG MODULE=central
ENV MODULE=${MODULE}

# Usar run.py directamente como entrypoint con el módulo
ENTRYPOINT ["sh", "-c", "python3 run.py ${MODULE}"]
