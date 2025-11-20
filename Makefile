# ===========================================================
# Makefile para sistema EVCharging (Kafka-only)
# Estructura del proyecto:
#   Makefile
#   Central/EV_Central.py
#   Cargador/EV_CP_E.py
#   Cargador/EV_CP_M.py
#   Driver/EV_Driver.py
#   config/config.yaml
# ===========================================================

PYTHON := python3

CP_ID ?= CP01
DRIVER_ID ?= D01
N ?= 1

# -----------------------------------------------------------
# HELP
# -----------------------------------------------------------

help:
	@echo "Comandos disponibles:"
	@echo "  make run-central"
	@echo "  make run-cp-engine CP_ID=CP01"
	@echo "  make run-cp-monitor CP_ID=CP01"
	@echo "  make run-driver DRIVER_ID=D01"
	@echo ""
	@echo "Ejecución múltiple:"
	@echo "  make run-cps N=3"
	@echo "  make run-drivers N=5"

# -----------------------------------------------------------
# EJECUCIÓN INDIVIDUAL
# -----------------------------------------------------------

run-central:
	@echo "[RUN] Central"
	$(PYTHON) Central/EV_Central.py

run-cp-engine:
	@echo "[RUN] CP Engine $(CP_ID)"
	$(PYTHON) Cargador/EV_CP_E.py $(CP_ID)

run-cp-monitor:
	@echo "[RUN] CP Monitor $(CP_ID)"
	$(PYTHON) Cargador/EV_CP_M.py $(CP_ID)

run-driver:
	@echo "[RUN] Driver $(DRIVER_ID)"
	$(PYTHON) Driver/EV_Driver.py $(DRIVER_ID)

# -----------------------------------------------------------
# EJECUCIÓN MÚLTIPLE AUTOMÁTICA
# -----------------------------------------------------------

run-cps:
	@echo "[RUN] Lanzando $(N) puntos de carga (Engine + Monitor)"
	@for i in $$(seq 1 $(N)); do \
		ID=$$(printf "CP%02d" $$i); \
		echo " - Lanzando $$ID"; \
		gnome-terminal --title="Engine $$ID" -- bash -c "make run-cp-engine CP_ID=$$ID; exec bash" & \
		gnome-terminal --title="Monitor $$ID" -- bash -c "make run-cp-monitor CP_ID=$$ID; exec bash" & \
	done

run-drivers:
	@echo "[RUN] Lanzando $(N) drivers"
	@for i in $$(seq 1 $(N)); do \
		ID=$$(printf "D%02d" $$i); \
		echo " - Lanzando Driver $$ID"; \
		gnome-terminal --title="Driver $$ID" -- bash -c "make run-driver DRIVER_ID=$$ID; exec bash" & \
	done

