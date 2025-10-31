"""
Script de setup y deployment para el sistema de producción multi-regional.
Configura la estructura de directorios y valida el entorno.
"""

import os
import sys
from pathlib import Path
from typing import List, Dict, Optional
import json
import argparse
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ProductionSetup:
    """Configurador del entorno de producción."""

    # Ya no es la “fuente de verdad”. Solo fallback al CREAR config si no existe.
    DEFAULT_REGIONS = ["LMR", "MAM", "MAR", "WCR", "NER", "GLR"]
    REQUIRED_SUBDIRS = ["Entradas", "Resultados"]

    def __init__(self, base_path: str = "reconciliation"):
        self.base_path = Path(base_path).resolve()
        self.config_file = self.base_path / "config.json"
        # Cargar config si existe (para obtener regiones desde allí)
        self.config: Dict = {}
        if self.config_file.exists():
            try:
                self.config = json.loads(self.config_file.read_text(encoding="utf-8"))
                logger.info(f"Config cargada: {self.config_file}")
            except Exception as e:
                logger.warning(f"No se pudo leer config.json ({e}). Se usarán fallbacks.")
        # Cache de regiones provenientes del config
        self.regions_from_config: List[str] = list(
            (self.config.get("system") or {}).get("regions", [])
        )

    def _get_regions_effective(self, regions: Optional[List[str]] = None) -> List[str]:
        """
        Devuelve las regiones efectivas a usar:
        - CLI (--regions) si vienen
        - config.json (system.regions) si existe
        - fallback DEFAULT_REGIONS como último recurso
        """
        if regions:
            return regions
        if self.regions_from_config:
            return self.regions_from_config
        return self.DEFAULT_REGIONS

    def create_directory_structure(self, regions: Optional[List[str]] = None) -> Dict[str, any]:
        """Crea la estructura de directorios completa."""
        regions = self._get_regions_effective(regions)

        setup_result = {
            "base_path": str(self.base_path),
            "created_dirs": [],
            "existing_dirs": [],
            "errors": []
        }

        logger.info(f"Creando estructura en: {self.base_path}")

        try:
            # Crear directorio base
            self.base_path.mkdir(parents=True, exist_ok=True)
            setup_result["created_dirs"].append(str(self.base_path))

            # Crear directorio de regiones
            regions_path = self.base_path / "Regions"
            regions_path.mkdir(exist_ok=True)

            # Crear estructura por región
            for region in regions:
                region_path = regions_path / region
                for subdir in self.REQUIRED_SUBDIRS:
                    full_path = region_path / subdir
                    if full_path.exists():
                        setup_result["existing_dirs"].append(str(full_path))
                    else:
                        full_path.mkdir(parents=True, exist_ok=True)
                        setup_result["created_dirs"].append(str(full_path))

            # Crear directorios adicionales
            additional_dirs = ["Logs", "Backups", "Config"]
            for dir_name in additional_dirs:
                dir_path = self.base_path / dir_name
                if not dir_path.exists():
                    dir_path.mkdir(exist_ok=True)
                    setup_result["created_dirs"].append(str(dir_path))

            logger.info(f"Estructura creada/validada. Directorios nuevos: {len(setup_result['created_dirs'])}")

        except Exception as e:
            setup_result["errors"].append(str(e))
            logger.error(f"Error creando estructura: {e}")

        return setup_result

    def create_config_file(self, config: Optional[Dict] = None, regions: Optional[List[str]] = None) -> Dict[str, any]:
        """Crea/actualiza archivo de configuración del sistema."""
        # Determinar regiones a escribir:
        # - CLI `regions` si vienen
        # - Si ya existe config y NO se pasa `regions`, mantener las actuales
        # - Si no existe config, usar DEFAULT_REGIONS como fallback
        if self.config_file.exists() and not regions:
            effective_regions = self.regions_from_config or self.DEFAULT_REGIONS
        else:
            effective_regions = regions or self.regions_from_config or self.DEFAULT_REGIONS

        default_config = {
            "system": {
                "name": "Regional Ticket Processing System",
                "version": "1.0.0",
                "base_path": str(self.base_path),
                "regions": effective_regions,
            },
            "processing": {
                "default_tolerance": 100,
                "default_huge_jump_threshold": 101,
                "default_engine": "pandas",
                "max_chunk_size": 10000
            },
            "monitoring": {
                "max_age_hours": 24,
                "backup_retention_days": 7,
                "health_check_interval": "daily"
            },
            "logging": {
                "level": "INFO",
                "log_file": "production_pipeline.log",
                "max_file_size": "10MB",
                "backup_count": 5
            },
            "notifications": {
                "email_enabled": False,
                "email_recipients": [],
                "alert_on_failure": True,
                "alert_on_stale_data": True
            }
        }

        if config:
            # Merge superficial; si quieres merge profundo, habría que hacerlo clave por clave.
            default_config.update(config)

        result = {
            "config_file": str(self.config_file),
            "config_created": False,
            "config_updated": False
        }

        try:
            if self.config_file.exists():
                logger.info(f"Actualizando configuración existente: {self.config_file}")
                result["config_updated"] = True
            else:
                logger.info(f"Creando archivo de configuración: {self.config_file}")
                result["config_created"] = True

            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2, ensure_ascii=False)

            # Actualizar cache en memoria para siguientes pasos en este mismo run
            self.config = default_config
            self.regions_from_config = list(effective_regions)

        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Error creando configuración: {e}")

        return result

    def validate_environment(self) -> Dict[str, any]:
        """Valida que el entorno esté correctamente configurado."""
        validation = {
            "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
            "required_packages": [],
            "missing_packages": [],
            "directory_structure": True,
            "config_exists": self.config_file.exists(),
            "issues": []
        }

        # Validar versión de Python
        if sys.version_info < (3, 8):
            validation["issues"].append(f"Python 3.8+ requerido, encontrado: {validation['python_version']}")

        # Validar paquetes requeridos
        required_packages = [
            "pandas", "numpy", "openpyxl", "pathlib"
        ]
        optional_packages = [
            "dask", "pyarrow"  # Para procesamiento distribuido
        ]

        for package in required_packages:
            try:
                __import__(package)
                validation["required_packages"].append(package)
            except ImportError:
                validation["missing_packages"].append(package)
                validation["issues"].append(f"Paquete requerido faltante: {package}")

        # Validar estructura de directorios contra regiones del config (o fallback)
        regions_to_check = self._get_regions_effective()
        regions_path = self.base_path / "Regions"
        if not regions_path.exists():
            validation["directory_structure"] = False
            validation["issues"].append("Estructura de directorios no encontrada")
        else:
            for region in regions_to_check:
                region_path = regions_path / region
                for subdir in self.REQUIRED_SUBDIRS:
                    if not (region_path / subdir).exists():
                        validation["directory_structure"] = False
                        validation["issues"].append(f"Directorio faltante: {region_path / subdir}")

        # Validar permisos de escritura
        try:
            test_file = self.base_path / ".write_test"
            test_file.write_text("test", encoding="utf-8")
            test_file.unlink()
        except Exception:
            validation["issues"].append("Sin permisos de escritura en directorio base")

        validation["is_valid"] = len(validation["issues"]) == 0
        return validation

    def generate_sample_data(self, region: str, fecha_tag: str, num_records: int = 1000) -> Path:
        """Genera archivo de ejemplo para pruebas."""
        import pandas as pd
        import numpy as np
        from datetime import datetime, timedelta

        # Generar datos de muestra
        plants = [f"Plant_{i:02d}" for i in range(1, 6)]  # 5 plantas

        data = []
        ticket_start = 10000

        for plant in plants:
            current_ticket = ticket_start
            for series in range(3):  # 3 series por planta
                series_length = np.random.randint(50, 200)
                for i in range(series_length):
                    if np.random.random() > 0.05:
                        data.append({
                            "Plant": plant,
                            "Ticket No": current_ticket,
                            "Date": (datetime.now() - timedelta(days=30) + timedelta(hours=i)).strftime("%Y-%m-%d"),
                            "Status": np.random.choice(["Completed", "In Progress", "Pending"]),
                            "Category": np.random.choice(["Maintenance", "Repair", "Inspection"])
                        })
                    current_ticket += 1
                current_ticket += np.random.randint(50, 200)
            ticket_start += 10000

        df = pd.DataFrame(data)

        entrada_path = self.base_path / "Regions" / region / "Entradas"
        filename = f"ticket_reconciliation_{region}_{fecha_tag}.xlsx"
        file_path = entrada_path / filename

        entrada_path.mkdir(parents=True, exist_ok=True)
        df.to_excel(file_path, index=False)

        logger.info(f"Archivo de muestra creado: {file_path} ({len(df)} registros)")
        return file_path


def main():
    """Función principal del script de setup."""
    parser = argparse.ArgumentParser(description="Setup del sistema de producción multi-regional")

    parser.add_argument("--base-path", default="Contuinity", help="Ruta base del sistema")
    parser.add_argument("--regions", nargs="*", help="Regiones específicas a configurar (si no, usa config o fallback)")
    parser.add_argument("--create-structure", action="store_true", help="Crear estructura de directorios")
    parser.add_argument("--create-config", action="store_true", help="Crear/actualizar archivo de configuración")
    parser.add_argument("--validate", action="store_true", help="Validar entorno")
    parser.add_argument("--generate-samples", action="store_true", help="Generar archivos de muestra")
    parser.add_argument("--fecha-tag", default="20241201", help="Tag de fecha para archivos de muestra")

    args = parser.parse_args()

    setup = ProductionSetup(args.base_path)

    print(f"🚀 Configurando sistema en: {setup.base_path}")
    print("=" * 60)

    # Crear estructura de directorios
    if args.create_structure:
        print("\n📁 Creando estructura de directorios...")
        result = setup.create_directory_structure(args.regions)
        print(f"✅ Directorios creados: {len(result['created_dirs'])}")
        print(f"ℹ️  Directorios existentes: {len(result['existing_dirs'])}")
        if result['errors']:
            print(f"❌ Errores: {len(result['errors'])}")
            for error in result['errors']:
                print(f"   - {error}")

    # Crear/actualizar configuración
    if args.create_config:
        print("\n⚙️  Creando archivo de configuración...")
        result = setup.create_config_file(regions=args.regions)
        if result.get('config_created'):
            print(f"✅ Configuración creada: {result['config_file']}")
        elif result.get('config_updated'):
            print(f"✅ Configuración actualizada: {result['config_file']}")
        if result.get('error'):
            print(f"❌ Error: {result['error']}")

    # Validar entorno
    if args.validate:
        print("\n🔍 Validando entorno...")
        validation = setup.validate_environment()
        print(f"🐍 Python: {validation['python_version']}")
        print(f"📦 Paquetes encontrados: {len(validation['required_packages'])}")
        print(f"📁 Estructura: {'✅' if validation['directory_structure'] else '❌'}")
        print(f"⚙️  Configuración: {'✅' if validation['config_exists'] else '❌'}")
        if validation['issues']:
            print(f"\n⚠️  Issues encontrados ({len(validation['issues'])}):")
            for issue in validation['issues']:
                print(f"   - {issue}")
        else:
            print("\n🎉 ¡Entorno válido y listo para producción!")

    # Generar archivos de muestra
    if args.generate_samples:
        print(f"\n📊 Generando archivos de muestra (fecha_tag: {args.fecha_tag})...")
        regions_to_process = args.regions or setup.regions_from_config or setup.DEFAULT_REGIONS
        for region in regions_to_process:
            try:
                file_path = setup.generate_sample_data(region, args.fecha_tag)
                print(f"✅ {region}: {file_path.name}")
            except Exception as e:
                print(f"❌ {region}: Error - {e}")

    print("\n" + "=" * 60)
    print("🏁 Setup completado")

    # Próximos pasos
    print("\n📋 Próximos pasos:")
    print("   1. Validar archivos de entrada en cada región")
    print("   2. Ejecutar procesamiento: python -m contuinity.production_system --regions LMR MAM")
    print("   3. Revisar salidas en Regions/{REGION}/Resultados/")
    print("   4. Configurar monitoreo automático si es necesario")


if __name__ == "__main__":
    main()

