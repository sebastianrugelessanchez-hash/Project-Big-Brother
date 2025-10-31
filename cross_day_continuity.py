# cross_day_continuity.py - REFACTORED VERSION
from __future__ import annotations
from pathlib import Path
from typing import Dict, Optional, Tuple, List
import re
import pandas as pd
import logging
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from functools import wraps
import time

from .io import read_table  # lectura robusta de Excel/CSV/Parquet
from .processing import clean_columns  # higiene básica de encabezados

logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURACIÓN Y CONSTANTES
# =============================================================================

@dataclass
class CrossDayConfig:
    """Configuración centralizada para análisis cross-day."""
    sheet_name: str = "Continuidad_Entre_Dias"  # Máximo 31 chars
    column_mappings: Dict[str, List[str]] = field(default_factory=lambda: {
        "plant": ["plant", "planta", "location"],
        "min": ["min", "min_ticket"], 
        "max": ["max", "max_ticket"]
    })
    max_gap_threshold: int = 1000
    file_pattern: str = "resultado_reconciliation_*{region}.xlsx"
    max_search_files: int = 100  # Límite para evitar scan masivo


@dataclass 
class ValidationResult:
    """Resultado de validación de datos."""
    is_valid: bool
    issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class AnalysisResult:
    """Resultado del análisis cross-day."""
    cross_day_df: Optional[pd.DataFrame]
    validation: ValidationResult
    metrics: Dict[str, any] = field(default_factory=dict)


# =============================================================================
# INTERFACES Y ABSTRACCIONES
# =============================================================================

class FileManager(ABC):
    """Interface para manejo de archivos - permite testing y diferentes backends."""
    
    @abstractmethod
    def find_previous_result_file(self, base_path: Path, region: str, 
                                 exclude_fecha_tag: Optional[str] = None) -> Optional[Path]:
        pass
    
    @abstractmethod
    def load_summary_sheet(self, file_path: Path) -> pd.DataFrame:
        pass


class ProductionFileManager(FileManager):
    """Implementación para producción - mantiene compatibilidad con sistema actual."""
    
    def __init__(self, config: CrossDayConfig):
        self.config = config
        
    def find_previous_result_file(self, base_path: Path, region: str, 
                                 exclude_fecha_tag: Optional[str] = None) -> Optional[Path]:
        """
        Versión optimizada que mantiene la misma lógica pero con mejor performance.
        """
        resultados_dir = base_path / "Regions" / region / "Resultados"
        if not resultados_dir.exists():
            return None

        pattern = self.config.file_pattern.format(region=region)
        candidates = list(resultados_dir.glob(pattern))
        
        # Límite de seguridad para evitar scan masivo
        if len(candidates) > self.config.max_search_files:
            logger.warning(f"Too many files ({len(candidates)}) in {resultados_dir}. "
                          f"Taking most recent {self.config.max_search_files}")
            # Pre-filtrar por fecha de modificación antes de procesar
            candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            candidates = candidates[:self.config.max_search_files]

        # Filtrar exclude_fecha_tag si se especifica
        if exclude_fecha_tag:
            pattern_exclude = re.compile(
                rf"resultado_reconciliation_{re.escape(exclude_fecha_tag)}{re.escape(region)}\.xlsx$", 
                re.IGNORECASE
            )
            candidates = [p for p in candidates if not pattern_exclude.search(p.name)]

        if not candidates:
            return None

        # Retornar el más reciente
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return candidates[0]
    
    def load_summary_sheet(self, file_path: Path) -> pd.DataFrame:
        """Carga la hoja Resumen con manejo robusto de errores."""
        try:
            df = read_table(str(file_path), sheet="Resumen", use_dask=False, header=0)
            return clean_columns(df)
        except Exception as e:
            logger.error(f"Failed to load summary sheet from {file_path}: {e}")
            raise


# =============================================================================
# UTILIDADES Y DECORADORES
# =============================================================================

def monitor_performance(func):
    """Decorador para monitorear performance de funciones críticas."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"{func.__name__} completed in {duration:.2f}s")
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"{func.__name__} failed after {duration:.2f}s: {e}")
            raise
    return wrapper


# =============================================================================
# CLASE PRINCIPAL
# =============================================================================

class CrossDayAnalyzer:
    """Analizador de continuidad entre días con arquitectura mejorada."""
    
    def __init__(self, 
                 file_manager: Optional[FileManager] = None,
                 config: Optional[CrossDayConfig] = None):
        self.config = config or CrossDayConfig()
        self.file_manager = file_manager or ProductionFileManager(self.config)
    
    def _get_column_names(self, df: pd.DataFrame) -> Tuple[str, str, str]:
        """
        Mapea nombres de columnas usando configuración flexible.
        Mantiene compatibilidad con lógica original pero más mantenible.
        """
        cols = {c.lower(): c for c in df.columns}
        
        # Buscar plant column
        plant_col = None
        for candidate in self.config.column_mappings["plant"]:
            if candidate in cols:
                plant_col = cols[candidate]
                break
        
        # Buscar min/max columns  
        min_col = None
        for candidate in self.config.column_mappings["min"]:
            if candidate in cols:
                min_col = cols[candidate]
                break
                
        max_col = None
        for candidate in self.config.column_mappings["max"]:
            if candidate in cols:
                max_col = cols[candidate]
                break
        
        # Fallback a defaults si no se encuentran
        return (
            plant_col or "Plant",
            min_col or "Min", 
            max_col or "Max"
        )
    
    def _validate_continuity_data(self, df: pd.DataFrame) -> ValidationResult:
        """Valida la consistencia de los datos para análisis de continuidad."""
        issues = []
        warnings = []
        
        try:
            # Validación de gaps excesivos
            if "Gap_Entre_Dias" in df.columns:
                max_gap = df["Gap_Entre_Dias"].max()
                if pd.notna(max_gap) and max_gap > self.config.max_gap_threshold:
                    warnings.append(f"Gap máximo ({max_gap}) excede threshold ({self.config.max_gap_threshold})")
            
            # Validación de tickets negativos
            numeric_cols = ["Ultimo_Ticket_Dia_Anterior", "Primer_Ticket_Dia_Actual"]
            for col in numeric_cols:
                if col in df.columns:
                    negative_count = (df[col] < 0).sum()
                    if negative_count > 0:
                        issues.append(f"Se encontraron {negative_count} valores negativos en {col}")
            
            # Validación de datos faltantes críticos
            if df.empty:
                issues.append("DataFrame vacío")
            elif "Plant" in df.columns and df["Plant"].isna().all():
                issues.append("Todas las plantas son NaN")
                
        except Exception as e:
            issues.append(f"Error durante validación: {e}")
        
        return ValidationResult(
            is_valid=len(issues) == 0,
            issues=issues,
            warnings=warnings
        )
    
    @monitor_performance
    def _build_cross_day_sheet(self, 
                              resumen_hoy: pd.DataFrame,
                              resumen_ayer: pd.DataFrame) -> pd.DataFrame:
        """
        Versión optimizada que mantiene la misma lógica de negocio.
        """
        # Obtener nombres de columnas
        plant_h, cmin_h, cmax_h = self._get_column_names(resumen_hoy)
        plant_a, cmin_a, cmax_a = self._get_column_names(resumen_ayer)

        # Preparar DataFrames base
        base_hoy = resumen_hoy[[plant_h, cmin_h]].rename(columns={
            plant_h: "Plant",
            cmin_h: "Primer_Ticket_Dia_Actual",
        })
        
        base_ayer = resumen_ayer[[plant_a, cmax_a]].rename(columns={
            plant_a: "Plant",
            cmax_a: "Ultimo_Ticket_Dia_Anterior",
        })

        # CORREGIR: Normalizar tipos de datos para el merge
        # Convertir ambas columnas Plant a string para garantizar compatibilidad
        base_hoy["Plant"] = base_hoy["Plant"].astype(str)
        base_ayer["Plant"] = base_ayer["Plant"].astype(str)

        # Merge optimizado
        merged = base_ayer.merge(base_hoy, on="Plant", how="outer")

        # Cálculos de continuidad
        merged["Iguales"] = (
            merged["Ultimo_Ticket_Dia_Anterior"] == merged["Primer_Ticket_Dia_Actual"]
        )
        merged["Gap_Entre_Dias"] = (
            merged["Primer_Ticket_Dia_Actual"] - merged["Ultimo_Ticket_Dia_Anterior"] - 1
        )
        merged["Continuidad_Estricta"] = (
            merged["Primer_Ticket_Dia_Actual"] == (merged["Ultimo_Ticket_Dia_Anterior"] + 1)
        )

        # Ordenar columnas según especificación original
        column_order = [
            "Plant",
            "Ultimo_Ticket_Dia_Anterior", 
            "Primer_Ticket_Dia_Actual",
            "Iguales",
            "Gap_Entre_Dias",
            "Continuidad_Estricta",
        ]
        
        # Asegurar que todas las columnas existen
        for col in column_order:
            if col not in merged.columns:
                if col.endswith(("Anterior", "Actual", "Dias")):
                    merged[col] = pd.Series(dtype="Int64")
                else:
                    merged[col] = pd.Series(dtype="object")

        return merged[column_order]
    
    def analyze(self, 
                current_sheets: Dict[str, pd.DataFrame],
                base_path: Path,
                region: str,
                fecha_tag_actual: str) -> AnalysisResult:
        """
        Método principal de análisis con manejo robusto de errores.
        """
        try:
            # Validar entrada
            if "Resumen" not in current_sheets:
                return AnalysisResult(
                    cross_day_df=None,
                    validation=ValidationResult(False, ["No se encontró hoja 'Resumen' en datos actuales"])
                )

            # Buscar archivo anterior
            prev_file = self.file_manager.find_previous_result_file(
                base_path, region, exclude_fecha_tag=fecha_tag_actual
            )
            
            if prev_file is None:
                logger.info(f"No se encontró día anterior para región {region}")
                return AnalysisResult(
                    cross_day_df=None,
                    validation=ValidationResult(True, warnings=["No hay día anterior disponible"])
                )

            # Cargar datos
            try:
                resumen_ayer = self.file_manager.load_summary_sheet(prev_file)
            except Exception as e:
                return AnalysisResult(
                    cross_day_df=None,
                    validation=ValidationResult(False, [f"Error cargando día anterior: {e}"])
                )

            resumen_hoy = clean_columns(current_sheets["Resumen"].copy())

            # Generar análisis
            cross_df = self._build_cross_day_sheet(resumen_hoy, resumen_ayer)
            
            # Validar resultados
            validation = self._validate_continuity_data(cross_df)
            
            # Métricas de análisis
            metrics = {
                "plants_analyzed": len(cross_df),
                "perfect_continuity": cross_df["Continuidad_Estricta"].sum() if "Continuidad_Estricta" in cross_df.columns else 0,
                "duplicates": cross_df["Iguales"].sum() if "Iguales" in cross_df.columns else 0,
                "gaps_found": (cross_df["Gap_Entre_Dias"] > 0).sum() if "Gap_Entre_Dias" in cross_df.columns else 0
            }
            
            logger.info(f"Cross-day analysis completed for {region}: {metrics}")
            
            return AnalysisResult(
                cross_day_df=cross_df,
                validation=validation,
                metrics=metrics
            )
            
        except Exception as e:
            logger.error(f"Unexpected error in cross-day analysis for {region}: {e}")
            return AnalysisResult(
                cross_day_df=None,
                validation=ValidationResult(False, [f"Error inesperado: {e}"])
            )


# =============================================================================
# INTERFAZ PÚBLICA - MANTIENE COMPATIBILIDAD TOTAL
# =============================================================================

# Instancia global para mantener compatibilidad
_default_analyzer = None

def get_default_analyzer() -> CrossDayAnalyzer:
    """Lazy initialization del analizador por defecto."""
    global _default_analyzer
    if _default_analyzer is None:
        _default_analyzer = CrossDayAnalyzer()
    return _default_analyzer


def enrich_sheets_with_cross_day(
    sheets: Dict[str, pd.DataFrame],
    base_path: str | Path,
    region: str,
    fecha_tag_actual: str
) -> Dict[str, pd.DataFrame]:
    """
    INTERFAZ PÚBLICA - MANTIENE COMPATIBILIDAD TOTAL CON PRODUCTION_SYSTEM.PY
    """
    analyzer = get_default_analyzer()

    result = analyzer.analyze(
        current_sheets=sheets,
        base_path=Path(base_path),
        region=region,
        fecha_tag_actual=fecha_tag_actual
    )

    # Logs de advertencias/errores
    if result.validation.warnings:
        for warning in result.validation.warnings:
            logger.warning(f"Cross-day analysis - {region}: {warning}")

    # Si no hay análisis cross-day (ej: no hay día anterior), devolvemos sin modificar
    if result.cross_day_df is None:
        return sheets

    # Si la validación falla, igual creamos una pestaña vacía para que siempre esté presente
    if not result.validation.is_valid:
        empty = pd.DataFrame(columns=[
            "Plant", "Ultimo_Ticket_Dia_Anterior", "Primer_Ticket_Dia_Actual",
            "Iguales", "Gap_Entre_Dias", "Continuidad_Estricta",
        ])
        new_sheets = sheets.copy()
        new_sheets[analyzer.config.sheet_name] = empty
        return new_sheets

    # Enriquecer sheets con los resultados
    new_sheets = sheets.copy()

    # 1) Añadir/actualizar la pestaña de continuidad entre días
    new_sheets[analyzer.config.sheet_name] = result.cross_day_df

    # 2) Añadir columna al Resumen actual (siempre conservar/crear 'Plant')
    try:
        resumen_actual = new_sheets["Resumen"].copy()
        plant_col, _, _ = analyzer._get_column_names(resumen_actual)

        # Info de continuidad (ya viene con 'Plant')
        continuity_info = result.cross_day_df[["Plant", "Iguales"]].rename(
            columns={"Iguales": "Continuidad_Dias"}
        )

        # Normalizar claves para el merge sin tocar tus columnas originales
        resumen_actual["_PlantKey"] = resumen_actual[plant_col].astype(str).str.strip()
        continuity_info["_PlantKey"] = continuity_info["Plant"].astype(str).str.strip()

        # Merge por la clave normalizada; no eliminamos tu columna original
        enhanced_resumen = resumen_actual.merge(
            continuity_info[["_PlantKey", "Continuidad_Dias"]],
            on="_PlantKey",
            how="left"
        ).drop(columns=["_PlantKey"])

        # Garantizar que SIEMPRE exista la columna 'Plant' visible
        if "Plant" not in enhanced_resumen.columns:
            # si tu columna de planta tenía otro nombre (planta/location), la clonamos como 'Plant'
            enhanced_resumen.insert(0, "Plant", enhanced_resumen[plant_col])

        new_sheets["Resumen"] = enhanced_resumen

    except Exception as e:
        logger.error(f"Error enriching Resumen sheet for {region}: {e}")
        # si algo falla, al menos mantenemos la pestaña nueva ya añadida
        pass

    return new_sheets


# =============================================================================
# LEGACY FUNCTIONS - MANTENER PARA COMPATIBILIDAD
# =============================================================================

# Estas funciones se mantienen para compatibilidad pero son deprecated
def _find_previous_result_file(base_path, region, exclude_fecha_tag=None):
    """DEPRECATED: Usar CrossDayAnalyzer en su lugar."""
    analyzer = get_default_analyzer()
    return analyzer.file_manager.find_previous_result_file(
        Path(base_path), region, exclude_fecha_tag
    )

def _get_cols(df):
    """DEPRECATED: Usar CrossDayAnalyzer._get_column_names en su lugar.""" 
    analyzer = get_default_analyzer()
    return analyzer._get_column_names(df)

def build_cross_day_sheet(resumen_hoy, resumen_ayer):
    """DEPRECATED: Usar CrossDayAnalyzer._build_cross_day_sheet en su lugar."""
    analyzer = get_default_analyzer() 
    return analyzer._build_cross_day_sheet(resumen_hoy, resumen_ayer)