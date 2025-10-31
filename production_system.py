"""
Sistema de producción para procesamiento de tickets por regiones.
Estructura: reconciliation/Regions/{REGION}/Entradas/ -> Resultados/
"""

from __future__ import annotations
import argparse
import logging
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
import re
import json

# Importar módulos del pipeline existente
from .io import read_table, write_excel, smart_read_excel
from .pipeline import run_pandas, run_dask
from .cross_day_continuity import enrich_sheets_with_cross_day

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('production_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def _load_config(config_path: str | None = None) -> dict:
    # Candidatos: 1) argumento --config, 2) junto al .py, 3) padre del paquete
    candidates = []
    if config_path:
        candidates.append(Path(config_path))
    here = Path(__file__).resolve().parent
    candidates.append(here / "config.json")        # ...\Codigo\config.json
    candidates.append(here.parent / "config.json") # ...\reconciliation\config.json

    for p in candidates:
        if p.exists():
            with open(p, "r", encoding="utf-8") as f:
                cfg = json.load(f)
            logging.info(f"✅ Config cargada desde: {p}")
            return cfg

    raise FileNotFoundError("No se encontró config.json en:\n" + "\n".join([str(c) for c in candidates]))


class RegionalProcessor:
    """Procesador de tickets por regiones siguiendo estructura estándar y config.json."""
    
    def __init__(self, base_path: str = "reconciliation", config_path: str | None = None):
        # Inicializa rutas base y path de regiones
        self.base_path = Path(base_path)
        self.regions_path = self.base_path / "Regions"
        self.regions_path.mkdir(parents=True, exist_ok=True)

        # Cargar config y soportar regiones
        self.config = _load_config(config_path)
        self.SUPPORTED_REGIONS: List[str] = list((self.config.get("system") or {}).get("regions", []))

    # ---------------- Descubrimiento / entrada-salida ----------------
    def discover_regions(self) -> List[str]:
        """Descubre subcarpetas bajo {base_path}/Regions como regiones válidas."""
        if not self.regions_path.exists():
            return []
        return [p.name for p in self.regions_path.iterdir() if p.is_dir()]

    def load_region_input(self, input_path: Path | str, sheet: Optional[str | int] = None, forced_header: Optional[int] = None) -> pd.DataFrame:
        """
        Loader reutilizable para archivos de entrada de una región.
        Reemplaza cualquier heurística local 0->1 que pudiera existir aquí.
        Devuelve un pandas.DataFrame.
        """
        return self._load_with_fallback(Path(input_path), sheet=sheet, forced_header=forced_header)

    def find_input_files(self, region: str, fecha_tag: Optional[str] = None) -> List[Path]:
        """
        Busca archivos de entrada en {base_path}/Regions/{region}/Entradas.
        Si fecha_tag se suministra filtra por presencia en el nombre.
        Retorna ordenado (más nuevo primero).
        """
        entradas_dir = self.regions_path / region / "Entradas"
        if not entradas_dir.exists():
            return []
        # Solo Excel
        files = [p for p in entradas_dir.iterdir() if p.is_file() and p.suffix.lower() in {".xlsx", ".xlsm", ".xls"}]
        if fecha_tag:
            files = [p for p in files if fecha_tag in p.name]
        files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return files

    def extract_fecha_tag(self, filename: str, region: str) -> Optional[str]:
        """Extrae el tag de fecha del nombre del archivo (acepta .xlsx, .xlsm, .xls)."""
        pattern = rf"ticket_reconciliation_{region}_(.+?)\.(xlsx|xlsm|xls)$"
        match = re.search(pattern, filename, re.IGNORECASE)
        return match.group(1) if match else None

    def generate_output_path(self, region: str, fecha_tag: str) -> Path:
        """Genera la ruta de salida siguiendo el estándar."""
        resultados_dir = self.regions_path / region / "Resultados"
        resultados_dir.mkdir(parents=True, exist_ok=True)
        output_filename = f"resultado_reconciliation_{fecha_tag}{region}.xlsx"
        return resultados_dir / output_filename

    # ----------------- Proceso por región / todas --------------------
    def process_region(
        self, 
        region: str, 
        fecha_tag: Optional[str] = None,
        tolerance: Optional[int] = None,
        huge_jump_threshold: Optional[int] = None,
        engine: Optional[str] = None
    ) -> Dict[str, any]:
        """
        Procesa una región específica usando parámetros del config.json si no se pasan por argumento.
        """
        # Tomar defaults desde config si vienen en None
        tolerance = tolerance if tolerance is not None else self.config["processing"]["default_tolerance"]
        huge_jump_threshold = (
            huge_jump_threshold if huge_jump_threshold is not None
            else self.config["processing"]["default_huge_jump_threshold"]
        )
        engine = (engine or self.config["processing"].get("default_engine", "pandas")).lower()

        logger.info(
            f"Iniciando procesamiento de región: {region} "
            f"(tol={tolerance}, huge={huge_jump_threshold}, engine={engine})"
        )
        
        try:
            input_files = self.find_input_files(region, fecha_tag)
            if not input_files:
                return {"region": region, "status": "error",
                        "message": f"No se encontraron archivos de entrada para {region}"}
            
            input_file = input_files[0]
            logger.info(f"Procesando archivo: {input_file}")
            
            actual_fecha_tag = self.extract_fecha_tag(input_file.name, region)
            if not actual_fecha_tag:
                return {"region": region, "status": "error",
                        "message": f"No se pudo extraer fecha_tag del archivo: {input_file.name}"}
            
            output_path = self.generate_output_path(region, actual_fecha_tag)

            # ----------------- Cargar y procesar datos -----------------
            n_rows = 0

            # Cargar Excel con heurística 0→1
            try:
                df = self._load_with_fallback(input_file)
                n_rows = int(df.shape[0]) if hasattr(df, "shape") else 0
            except Exception as e:
                logger.warning(f"No se pudo cargar {input_file.name} con smart_read_excel/read_table: {e}")
                raise

            # Dask solo para CSV/Parquet (Excel no)
            if engine == "dask" and input_file.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
                raise ValueError("Dask no soporta Excel en este flujo. Usa --engine pandas o convierte a CSV/Parquet.")

            if engine == "dask":
                # Si alguna vez lees CSV/Parquet con Dask, cargarías así:
                ddf = read_table(str(input_file), use_dask=True, header=0)  # para CSV; Parquet ignora header
                resumen, detalle, faltantes, duplicados = run_dask(ddf, tolerance, huge_jump_threshold)
            else:
                resumen, detalle, faltantes, duplicados = run_pandas(df, tolerance, huge_jump_threshold)

            sheets = {
                "Resumen": resumen,
                "Detalle_por_Serie": detalle,
                "Faltantes_intra": faltantes,
                "Duplicados_por_Serie": duplicados,
            }

            # Enriquecer con comparación AYER vs HOY:
            sheets = enrich_sheets_with_cross_day(
                sheets=sheets,
                base_path=self.base_path,
                region=region,
                fecha_tag_actual=actual_fecha_tag,
            )
            
            write_excel(output_path, sheets)
            logger.info(f"✅ Región {region} procesada exitosamente -> {output_path}")
            
            return {
                "region": region,
                "status": "success",
                "input_file": str(input_file),
                "output_file": str(output_path),
                "fecha_tag": actual_fecha_tag,
                "records_processed": n_rows,
                "summary": {
                    "plantas": len(resumen),
                    "series_total": len(detalle),
                    "faltantes_series": len(faltantes),
                    "duplicados_series": len(duplicados)
                }
            }
        except Exception as e:
            logger.error(f"Error procesando región {region}: {str(e)}")
            return {"region": region, "status": "error", "message": str(e)}
    
    def _load_with_fallback(self, file_path: Path, sheet: Optional[str | int] = None, forced_header: Optional[int] = None) -> pd.DataFrame:
        """
        Carga archivo con estrategia de fallback:
         1) smart_read_excel (heurística 0→1)
         2) read_table (io.read_table) forzando pandas
        Devuelve un pandas.DataFrame.
        """
        # 1) smart_read_excel (preferido)
        try:
            df = smart_read_excel(str(file_path), sheet=sheet, forced_header=forced_header)
            logger.info(f"Cargado con smart_read_excel: {file_path.name}")
            return df
        except Exception as e:
            logger.debug(f"smart_read_excel falló para {file_path.name}: {e}")

        # 2) read_table (sin dask) -- forzamos pandas
        df = read_table(str(file_path), use_dask=False, sheet=sheet, header=0)
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)
        logger.info(f"Cargado con read_table: {file_path.name}")
        return df
    
    def process_all_regions(
        self, 
        regions: Optional[List[str]] = None,
        fecha_tag: Optional[str] = None,
        tolerance: Optional[int] = None,
        huge_jump_threshold: Optional[int] = None,
        engine: Optional[str] = None
    ) -> Dict[str, Dict]:
        """Procesa todas las regiones disponibles o las especificadas."""
        if regions is None:
            # Auto-descubre y filtra por las soportadas en config.json
            discovered = self.discover_regions()
            regions = [r for r in discovered if (not self.SUPPORTED_REGIONS) or r in self.SUPPORTED_REGIONS]
        else:
            regions = [r for r in regions if (not self.SUPPORTED_REGIONS) or r in self.SUPPORTED_REGIONS]

        if not regions:
            logger.warning("Ninguna región válida tras filtrar contra config.json")
            return {}
        
        results = {}
        success_count = 0
        logger.info(f"Iniciando procesamiento de {len(regions)} regiones: {regions}")
        
        for region in regions:
            result = self.process_region(
                region=region,
                fecha_tag=fecha_tag,
                tolerance=tolerance,
                huge_jump_threshold=huge_jump_threshold,
                engine=engine
            )
            results[region] = result
            if result.get("status") == "success":
                success_count += 1
        
        logger.info(f"Procesamiento completado: {success_count}/{len(regions)} regiones exitosas")
        return results
    
    def generate_consolidated_report(self, results: Dict[str, Dict]) -> pd.DataFrame:
        """Genera reporte consolidado de todas las regiones procesadas."""
        report_data = []
        for region, result in results.items():
            if result["status"] == "success":
                summary = result.get("summary", {})
                report_data.append({
                    "Region": region,
                    "Status": "✅ Exitoso",
                    "Fecha_Tag": result.get("fecha_tag", "N/A"),
                    "Registros_Procesados": result.get("records_processed", 0),
                    "Plantas": summary.get("plantas", 0),
                    "Series_Total": summary.get("series_total", 0),
                    "Faltantes_Series": summary.get("faltantes_series", 0),
                    "Duplicados_Series": summary.get("duplicados_series", 0),
                    "Archivo_Salida": Path(result.get("output_file", "")).name
                })
            else:
                report_data.append({
                    "Region": region,
                    "Status": f"❌ Error: {result.get('message', 'Desconocido')}",
                    "Fecha_Tag": "N/A",
                    "Registros_Procesados": 0,
                    "Plantas": 0,
                    "Series_Total": 0,
                    "Faltantes_Series": 0,
                    "Duplicados_Series": 0,
                    "Archivo_Salida": "N/A"
                })
        return pd.DataFrame(report_data)


def main():
    """Función principal para ejecución desde línea de comandos."""
    parser = argparse.ArgumentParser(
        description="Sistema de producción para procesamiento multi-regional de tickets"
    )
    parser.add_argument("--base-path", default="reconciliation", help="Ruta base del proyecto")
    parser.add_argument("--config", help="Ruta alternativa a config.json", default=None)
    parser.add_argument("--regions", nargs="*", help="Regiones específicas a procesar (si no, auto-descubre)")
    parser.add_argument("--fecha-tag", help="Tag de fecha específico a procesar (default: más reciente)")
    parser.add_argument("--tolerance", type=int, default=None, help="Override tolerancia (si no, config)")
    parser.add_argument("--huge", type=int, default=None, help="Override umbral 'Huge Jump' (si no, config)")
    parser.add_argument("--engine", choices=["pandas", "dask"], default=None, help="Override engine (si no, config)")
    parser.add_argument("--report", help="Ruta para guardar reporte consolidado (opcional)")
    
    args = parser.parse_args()
    
    processor = RegionalProcessor(args.base_path, config_path=args.config)
    
    results = processor.process_all_regions(
        regions=args.regions,
        fecha_tag=args.fecha_tag,
        tolerance=args.tolerance,
        huge_jump_threshold=args.huge,
        engine=args.engine
    )
    
    if results:
        report_df = processor.generate_consolidated_report(results)
        print("\n" + "="*80)
        print("REPORTE CONSOLIDADO DE PROCESAMIENTO")
        print("="*80)
        print(report_df.to_string(index=False))
        
        if args.report:
            report_path = Path(args.report)
            report_path.parent.mkdir(parents=True, exist_ok=True)
            write_excel(report_path, {"Reporte_Consolidado": report_df})
            print(f"\n📊 Reporte guardado en: {report_path}")
    else:
        print("❌ No se procesaron regiones")


if __name__ == "__main__":
    main()
