#!/usr/bin/env python3
"""
Punto de entrada principal para la ejecución de todos los módulos del sistema EVCharging
Uso:
    python run.py central
    python run.py cp_engine --id CP01
    python run.py cp_monitor --id CP01
    python run.py driver --id D01
"""

import sys
import argparse
import os

# Añadir el directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def main():
    parser = argparse.ArgumentParser(description='Sistema EVCharging - SD 25/26')
    parser.add_argument('module', choices=['central', 'cp_engine', 'cp_monitor', 'driver'],
                        help='Módulo a ejecutar')
    parser.add_argument('--id', type=str, help='ID del módulo (requerido para cp_engine, cp_monitor y driver)')
    
    args = parser.parse_args()
    
    if args.module == 'central':
        from Central.EV_Central import main as central_main
        central_main()
    
    elif args.module == 'cp_engine':
        if not args.id:
            print("Error: --id es requerido para cp_engine")
            sys.exit(1)
        from Cargador.EV_CP_E import main as engine_main
        engine_main(args.id)
    
    elif args.module == 'cp_monitor':
        if not args.id:
            print("Error: --id es requerido para cp_monitor")
            sys.exit(1)
        from Cargador.EV_CP_M import main as monitor_main
        monitor_main(args.id)
    
    elif args.module == 'driver':
        if not args.id:
            print("Error: --id es requerido para driver")
            sys.exit(1)
        from Driver.EV_Driver import main as driver_main
        driver_main(args.id)


if __name__ == '__main__':
    main()
