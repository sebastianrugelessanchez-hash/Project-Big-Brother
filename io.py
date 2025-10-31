from __future__ import annotations 
from pathlib import Path
from typing import Optional, Iterable, Union, Any, TYPE_CHECKING
import pandas as pd

if TYPE_CHECKING:
    import dask.dataframe as dd
    DaskDF = dd.DataFrame
else:
    DaskDF = Any

def read_table(
    path: Union[str, Path],
    sheet: Optional[Union[str, int]] = None,
    use_dask: bool = False,
    chunksize: Optional[int] = None,
    header: int = 1,   # <-- por defecto primera fila como header
) -> Union[pd.DataFrame, DaskDF, Iterable[pd.DataFrame]]:
    p = Path(path)
    ext = p.suffix.lower()

    # --- Excel: usar header ---
    if ext in {".xlsx", ".xls", ".xlsm"}:
        return pd.read_excel(p, sheet_name=sheet, header=header)

    # --- CSV ---
    if ext in {".csv", ".txt"}:
        if use_dask:
            try:
                import dask.dataframe as dd  # type: ignore
            except Exception as e:
                raise RuntimeError("Dask no está instalado. `conda install -c conda-forge dask[complete]`") from e
            # Dask también acepta header
            return dd.read_csv(str(p), header=header, assume_missing=True, blocksize="64MB")
        # pandas CSV con/sin chunks respetando header
        if chunksize:
            return pd.read_csv(p, header=header, chunksize=chunksize)
        return pd.read_csv(p, header=header)

    # --- Parquet (sin header) ---
    if ext == ".parquet":
        if use_dask:
            try:
                import dask.dataframe as dd  # type: ignore
            except Exception as e:
                raise RuntimeError("Dask no está instalado. `conda install -c conda-forge dask[complete]`") from e
            return dd.read_parquet(str(p))
        return pd.read_parquet(p)

    raise ValueError(f"Extensión de archivo no soportada: {ext}")


def write_excel(path: Union[str, Path], sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(path) as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)


# =====================================================
# Nuevo: Loader inteligente para Excel con heurística 0→1
# =====================================================
from .processing import clean_columns, detect_columns

def smart_read_excel(
    path: Union[str, Path],
    sheet: Optional[Union[str, int]] = None,
    forced_header: Optional[int] = None,
) -> pd.DataFrame:
    """
    Lee un archivo Excel con heurística de encabezado:
      - Si forced_header es int, lo usa directo.
      - Si None: intenta con header=0 y luego header=1.
      - Valida columnas clave con detect_columns.
    Retorna siempre un pandas.DataFrame limpio.
    """
    p = str(path)

    # detectar hoja si no se pasa
    if sheet is None:
        try:
            with pd.ExcelFile(p) as xls:
                sheet = xls.sheet_names[0]
        except Exception:
            sheet = 0

    def _ensure_df(x):
        if isinstance(x, dict):
            return x.get("Sheet1", next(iter(x.values())))
        return x

    # Caso forced_header explícito
    if forced_header is not None:
        df = _ensure_df(read_table(p, sheet=sheet, use_dask=False, header=forced_header))
        df = clean_columns(df).loc[:, ~df.columns.duplicated()]
        detect_columns(df)
        return df

    last_err = None
    for hdr in (0, 1):
        try:
            df_try = _ensure_df(read_table(p, sheet=sheet, use_dask=False, header=hdr))
            df_try = clean_columns(df_try).loc[:, ~df_try.columns.duplicated()]
            detect_columns(df_try)
            return df_try
        except Exception as e:
            last_err = e

    # Diagnóstico si no funcionó
    cols0 = [str(c) for c in _ensure_df(read_table(p, sheet=sheet, use_dask=False, header=0)).columns.tolist()]
    cols1 = [str(c) for c in _ensure_df(read_table(p, sheet=sheet, use_dask=False, header=1)).columns.tolist()]
    raise SystemExit(
        "No se detectaron columnas clave con header=0 ni header=1.\n"
        f"- Columnas con header=0: {cols0}\n"
        f"- Columnas con header=1: {cols1}\n"
        f"Último error: {type(last_err).__name__}: {last_err}"
    )
