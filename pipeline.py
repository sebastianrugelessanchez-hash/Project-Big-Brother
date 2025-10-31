"""
Pipeline de alto nivel para ejecutar el flujo con pandas o Dask.
"""
from __future__ import annotations
from typing import Tuple, TYPE_CHECKING, Any, Optional
import pandas as pd

# Alias de tipo seguro para Dask DataFrame
if TYPE_CHECKING:
    import dask.dataframe as dd
    DaskDF = dd.DataFrame
else:
    DaskDF = Any

try:
    import dask.dataframe as dd  # type: ignore
except Exception:  # pragma: no cover
    dd = None  # type: ignore

from .processing import clean_columns, detect_columns, coerce_core
from .series import (
    detect_series, aggregate_by_series, compute_inter_series_gaps,
    duplicates_by_series, summarize_by_plant
)

# ----------------- helpers -----------------
def _ensure_col(df: pd.DataFrame, std_name: str, variants: list[str], dtype: str = "object") -> pd.DataFrame:
    """Si existe alguna variante en df.columns, la renombra a std_name; si no, la crea vacía."""
    for v in variants:
        if v in df.columns:
            if v != std_name:
                df = df.rename(columns={v: std_name})
            return df
    df[std_name] = pd.Series(dtype=dtype)
    return df

def _format_plant(df: pd.DataFrame, plant_col: str) -> pd.DataFrame:
    """Asegura que la columna de planta sea texto y sin sufijo '.0'; limpia 'nan'/'None' y espacios."""
    if plant_col not in df.columns:
        return df
    s = df[plant_col].astype(str).str.strip()
    s = s.replace({"nan": "", "None": ""})
    s = s.str.replace(r"\.0$", "", regex=True)
    df[plant_col] = s
    return df

def _drop_nan_plant(df: pd.DataFrame, plant_col: str) -> pd.DataFrame:
    """Elimina filas donde la planta es vacía tras el formateo."""
    if plant_col not in df.columns:
        return df
    mask = df[plant_col].astype(str).str.strip().ne("")
    return df.loc[mask].reset_index(drop=True)

# -------- faltantes intra robusto ----------
def _faltantes_intra_group(g: pd.DataFrame, plant_col: str, ticket_col: str) -> pd.DataFrame:
    """Detecta tickets faltantes entre consecutivos dentro de un grupo (Plant, series_id)."""
    t = (
        g[ticket_col]
        .dropna()
        .drop_duplicates()
        .astype("int64")
        .to_numpy()
    )
    if t.size <= 1:
        return pd.DataFrame(columns=[plant_col, "series_id", "Tickets Involucrados", "Tickets_missing_serie"])

    t.sort()
    gaps, invs = [], []
    for i in range(len(t) - 1):
        if t[i + 1] - t[i] > 1:
            falt = list(range(t[i] + 1, t[i + 1]))
            gaps.extend(falt)
            invs.append(f"{t[i]}–{t[i + 1]}")

    if not gaps:
        return pd.DataFrame(columns=[plant_col, "series_id", "Tickets Involucrados", "Tickets_missing_serie"])

    return pd.DataFrame({
        plant_col: [g[plant_col].iloc[0]] * len(gaps),
        "series_id": [g["series_id"].iloc[0]] * len(gaps),
        "Tickets Involucrados": [", ".join(invs)] * len(gaps),
        "Tickets_missing_serie": gaps,
    })
# -------------------------------------------


def run_pandas(
    df: pd.DataFrame,
    tolerance: int,
    huge_jump_threshold: int
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Limpieza y detección de columnas clave
    df = clean_columns(df)
    plant_col, ticket_col = detect_columns(df)

    # Normalización de núcleo + series
    core = coerce_core(df, plant_col, ticket_col)
    core = detect_series(core, plant_col, ticket_col, tolerance)

    # Agregados por serie e inter-series
    agg_series = aggregate_by_series(core, plant_col, ticket_col)
    inter = compute_inter_series_gaps(agg_series, plant_col, huge_jump_threshold)

    # Faltantes intra por serie (robusto)
    core = core.copy()
    core[ticket_col] = pd.to_numeric(core[ticket_col], errors="coerce").astype("Int64")
    core[plant_col]  = core[plant_col].astype(str).str.strip().str.replace(r"\.0$", "", regex=True)

    falt_intra = (
        core.sort_values([plant_col, "series_id", ticket_col])
            .groupby([plant_col, "series_id"], observed=True, sort=False)
            .apply(lambda g: _faltantes_intra_group(g, plant_col, ticket_col))
            .reset_index(drop=True)
    )

    # Normalizar nombres esperados
    falt_intra = _ensure_col(
        falt_intra, "Tickets Involucrados",
        ["Tickets Involucrados", "Tickets_involucrados", "Tickets Involved", "Tickets_involved"]
    )
    falt_intra = _ensure_col(
        falt_intra, "Tickets_missing_serie",
        ["Tickets_missing_serie", "Tickets missing serie", "Tickets faltantes serie", "Tickets_missing_series"]
    )
    falt_intra = _ensure_col(
        falt_intra, "series_id",
        ["series_id", "Serie", "Series", "serie", "series"], dtype="Int64"
    )
    falt_intra = _ensure_col(
        falt_intra, plant_col, [plant_col, "Plant", "planta", "location", "loc", "site"]
    )
    cols_falt = [plant_col, "series_id", "Tickets Involucrados", "Tickets_missing_serie"]
    falt_intra = falt_intra.reindex(columns=[c for c in cols_falt if c in falt_intra.columns])

    # Duplicados por serie
    dups = duplicates_by_series(core, plant_col, ticket_col)

    # Resumen por planta
    resumen = summarize_by_plant(agg_series, inter, plant_col)

    # Detalle por serie enriquecido
    det_serie = agg_series.merge(
        inter[[plant_col, "series_id", "Inter_Len", "Huge Jump"]],
        on=[plant_col, "series_id"], how="left"
    )

    # Formateo de Plant y limpieza de filas vacías en todas las salidas
    resumen    = _drop_nan_plant(_format_plant(resumen,    plant_col), plant_col)
    det_serie  = _drop_nan_plant(_format_plant(det_serie,  plant_col), plant_col)
    falt_intra = _drop_nan_plant(_format_plant(falt_intra, plant_col), plant_col)
    dups       = _drop_nan_plant(_format_plant(dups,       plant_col), plant_col)

    return resumen, det_serie, falt_intra, dups


def run_dask(
    ddf: "DaskDF",
    tolerance: int,
    huge_jump_threshold: int
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if dd is None:
        raise RuntimeError("Dask no está disponible.")
    df = ddf.compute()
    return run_pandas(df, tolerance, huge_jump_threshold)


# =======================
# Dispatcher (al final)
# =======================
from pathlib import Path as _Path
import pandas as _pd
from .io import read_table

def _load_input_table(
    source: Any,
    sheet: Optional[str | int] = None,
    forced_header: Optional[int] = None,
    use_dask: bool = False,
    chunksize: Optional[int] = None,
) -> Any:
    """
    Carga el input de forma centralizada:
    - Si 'source' ya es DataFrame o dict lo retorna tal cual.
    - Si es path (str/Path) delega en io.read_table (sin repetir heurísticas).
    Devuelve pandas.DataFrame, dict (sheets), Dask DataFrame o iterable de DataFrames (chunks).
    """
    if isinstance(source, (_pd.DataFrame, dict)):
        return source

    path_str = str(source) if not isinstance(source, _Path) else str(source)
    header_val = forced_header if forced_header is not None else 0

    tbl = read_table(
        path_str,
        sheet=sheet,
        use_dask=use_dask,
        chunksize=chunksize,
        header=header_val,
    )

    if isinstance(tbl, dict):
        if "Sheet1" in tbl:
            return tbl["Sheet1"]
        return next(iter(tbl.values()))
    return tbl


def run_pipeline(
    input_source: Any,
    sheet: Optional[str | int] = None,
    forced_header: Optional[int] = None,
    tolerance: int = 100,
    huge_jump_threshold: int = 101,
    use_dask: bool = False,
    chunksize: Optional[int] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Entrada única para ejecutar el pipeline.
    - Delegado de lectura a io.read_table via _load_input_table.
    - Si el input es Dask DF -> run_dask; si es pandas DF -> run_pandas;
      si es iterable de chunks -> ejecutar por chunks y consolidar.
    """
    loaded = _load_input_table(
        input_source,
        sheet=sheet,
        forced_header=forced_header,
        use_dask=use_dask,
        chunksize=chunksize,
    )

    if dd is not None and hasattr(dd, "DataFrame"):
        try:
            from dask.dataframe import DataFrame as _DaskDF  # type: ignore
            if isinstance(loaded, _DaskDF):  # type: ignore
                return run_dask(loaded, tolerance, huge_jump_threshold)
        except Exception:
            pass

    if hasattr(loaded, "__iter__") and not isinstance(loaded, _pd.DataFrame):
        agg_resumen, agg_detalle, agg_falt, agg_dups = [], [], [], []
        for chunk in loaded:
            r, d, f, du = run_pandas(chunk, tolerance, huge_jump_threshold)
            agg_resumen.append(r); agg_detalle.append(d)
            agg_falt.append(f);   agg_dups.append(du)
        resumen    = _pd.concat(agg_resumen, ignore_index=True) if agg_resumen else _pd.DataFrame()
        detalle    = _pd.concat(agg_detalle, ignore_index=True) if agg_detalle else _pd.DataFrame()
        faltantes  = _pd.concat(agg_falt,   ignore_index=True) if agg_falt   else _pd.DataFrame()
        duplicados = _pd.concat(agg_dups,   ignore_index=True) if agg_dups   else _pd.DataFrame()
        return resumen, detalle, faltantes, duplicados

    if isinstance(loaded, _pd.DataFrame):
        return run_pandas(loaded, tolerance, huge_jump_threshold)

    raise TypeError(f"Tipo de dato no soportado por el pipeline: {type(loaded)}")
