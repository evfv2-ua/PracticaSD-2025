# EVCharging (Kafka) - Guía rápida de despliegue en varias máquinas

## 1 Kafka (máquina Central)
1. Edita `config/config.yaml` y pon la IP de la máquina que tendrá Kafka/Central:  
   ```yaml
   kafka:
     broker: "10.0.0.5:9092"   # ejemplo
   ```
2. Exporta la IP antes de levantar Kafka para que se anuncie correctamente:  
   ```bash
   export HOST_IP=10.0.0.5
   docker compose up -d kafka zookeeper
   ```
   (El `docker-compose.yml` ya usa `HOST_IP` en `KAFKA_ADVERTISED_LISTENERS`.)
3. Arranca la central (mismo host):  
   ```bash
   export KAFKA_BROKER=10.0.0.5:9092
   make run-central
   ```
   Opcional: interfaz web (requiere API en 8000): `docker compose up web` o `cd web && npm install && npm run dev -- --host`.

## 2 Máquinas de Cargadores (Engine/Monitor)
En cada PC de CP:  
```bash
export KAFKA_BROKER=10.0.0.5:9092
make run-cp-engine CP_ID=CP01
make run-cp-monitor CP_ID=CP01
```
Repite con CP02, etc. (Si usas Docker para CP, ajusta el broker igual.)

## 3 Máquinas de Drivers
En cada PC de driver:  
```bash
export KAFKA_BROKER=10.0.0.5:9092
make run-driver DRIVER_ID=D01
```

## 4 Notas importantes de conectividad
- Asegúrate de que el puerto 9092 está abierto (firewall) y la IP es alcanzable por el resto de PCs.
- No uses `localhost` en `config.yaml` cuando los clientes están en otras máquinas; siempre la IP del host donde corre Kafka.
- Si cambias la IP o el broker, reinicia los procesos para que recojan la variable de entorno o el config.
- La API web de Central expone `http://<central>:8000`; si usas el frontend, define `VITE_API_URL` acorde.

## 5 Comandos útiles (Makefile)
- `make run-central`
- `make run-cp-engine CP_ID=CP01`
- `make run-cp-monitor CP_ID=CP01`
- `make run-driver DRIVER_ID=D01`
- `make web-install | web-dev | web-build`

## 6 Docker opcional del frontend
```bash
docker compose build web
docker compose up web   # expone http://<central>:4173
```
