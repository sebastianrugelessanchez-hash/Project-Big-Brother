from __future__ import annotations
import argparse
from pathlib import Path
from typing import Iterable, Tuple, Optional
import pandas as pd

from .io import read_table, write_excel
from .pipeline import run_pipeline
from .processing import clean_columns, detect_columns  # validación de headers


def _is_excel(path: str) -> bool:
    p = path.lower()
    return p.endswith(".xlsx") or p.endswith(".xls") or p.endswith(".xlsm")


def _is_csv(path: str) -> bool:
    return path.lower().endswith(".csv")


def _is_parquet(path: str) -> bool:
    return path.lower().endswith(".parquet")


def _process_chunks(
    chunks: Iterable[pd.DataFrame],
    tolerance: int,
    huge_jump_threshold: int,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Procesa CSV grande en chunks sin cargar todo a memoria.
    Consolida al final: Resumen (agg), Detalle, Faltantes, Duplicados (concat).
    """
    agg_resumen, agg_detalle, agg_falt, agg_dups = [], [], [], []

    for chunk in chunks:
        # Limpieza mínima por chunk para mantener una sola normalización
        chunk = clean_columns(chunk)
        r, d, f, du = run_pipeline(
            chunk,
            tolerance=tolerance,
            huge_jump_threshold=huge_jump_threshold,
        )
        agg_resumen.append(r); agg_detalle.append(d)
        agg_falt.append(f);   agg_dups.append(du)

    if agg_resumen:
        resumen = pd.concat(agg_resumen, ignore_index=True)
        group_cols = [c for c in resumen.columns if c.lower() in ("plant", "planta", "location")]
        group_cols = group_cols or ["plant"]
        agg_dict = {}
        if "Count" in resumen.columns: agg_dict["Count"] = "sum"
        if "Min"   in resumen.columns: agg_dict["Min"]   = "min"
        if "Max"   in resumen.columns: agg_dict["Max"]   = "max"
        if not agg_dict:
            agg_dict = {c: "first" for c in resumen.columns if c not in group_cols}
        resumen = resumen.groupby(group_cols, as_index=False).agg(agg_dict)
    else:
        resumen = pd.DataFrame()

    detalle    = pd.concat(agg_detalle, ignore_index=True) if agg_detalle else pd.DataFrame()
    faltantes  = pd.concat(agg_falt,   ignore_index=True) if agg_falt   else pd.DataFrame()
    duplicados = pd.concat(agg_dups,   ignore_index=True) if agg_dups   else pd.DataFrame()
    return resumen, detalle, faltantes, duplicados


def _load_excel_with_fallback(path: str, sheet: Optional[str | int], forced_header: Optional[int]) -> pd.DataFrame:
    """
    Si forced_header es un entero, lee con ese header.
    Si es None, intenta header=0 y luego header=1; valida columnas clave.
    Si sheet es None y es Excel, toma la primera hoja automáticamente.
    """
    # Si es Excel y no nos dieron hoja, tomar la primera
    if _is_excel(path) and sheet is None:
        try:
            with pd.ExcelFile(path) as xls:
                sheet = xls.sheet_names[0]  # primera hoja
        except Exception:
            sheet = 0  # fallback por índice

    def _ensure_df(x):
        # Si read_table devolvió un dict de DataFrames (sheet=None), tomar el primero
        if isinstance(x, dict):
            if "Sheet1" in x:      # prioriza la típica
                return x["Sheet1"]
            return next(iter(x.values()))
        return x

    if forced_header is not None:
        df = read_table(path, sheet=sheet, use_dask=False, header=forced_header)
        df = _ensure_df(df)
        df = clean_columns(df)
        df = df.loc[:, ~df.columns.duplicated()]
        return df

    last_err: Exception | None = None
    for hdr in (0, 1):
        df_try = read_table(path, sheet=sheet, use_dask=False, header=hdr)
        df_try = _ensure_df(df_try)
        df_try = clean_columns(df_try)
        df_try = df_try.loc[:, ~df_try.columns.duplicated()]
        try:
            detect_columns(df_try)   # valida columnas clave (Plant/Ticket con variantes)
            return df_try
        except Exception as e:
            last_err = e

    # Diagnóstico si no funcionó
    cols0 = [str(c) for c in _ensure_df(read_table(path, sheet=sheet, use_dask=False, header=0)).columns.tolist()]
    cols1 = [str(c) for c in _ensure_df(read_table(path, sheet=sheet, use_dask=False, header=1)).columns.tolist()]
    raise SystemExit(
        "No se detectaron columnas clave con header=0 ni header=1.\n"
        f"- Columnas con header=0: {cols0}\n"
        f"- Columnas con header=1: {cols1}\n"
        f"Último error: {type(last_err).__name__}: {last_err}"
    )


def main():
    ap = argparse.ArgumentParser(description="Reconciliation data processing pipeline")
    ap.add_argument("--in", dest="input_path", required=True, help="Ruta del archivo de entrada (Excel/CSV/Parquet)")
    ap.add_argument("--out", dest="output_path", required=True, help="Ruta del Excel de salida")
    ap.add_argument("--sheet", dest="sheet", default=None, help="Nombre o índice de la hoja (solo Excel)")
    ap.add_argument("--tolerance", type=int, default=100, help="Tolerancia para separar series")
    ap.add_argument("--huge", type=int, default=101, help="Umbral de 'Huge Jump' inter-series (longitud de hueco)")
    ap.add_argument("--engine", choices=["pandas", "dask"], default="pandas", help="Motor de ejecución")
    ap.add_argument("--chunksize", type=int, default=None, help="Tamaño de chunk para CSV con pandas")
    ap.add_argument("--header", type=int, default=None, help="Forzar fila de encabezado (0=primera, 1=segunda, ...). Por defecto AUTO 0→1")

    args = ap.parse_args()

    base_path = args.base_path
    in_path   = args.input_path
    out_path  = args.output_path
    sheet     = args.sheet
    tol       = args.tolerance
    huge      = args.huge
    engine    = args.engine
    chunksize = args.chunksize
    forced_header = args.header

    # Ajustar rutas relativas al base_path si es necesario
    if not Path(in_path).is_absolute():
        in_path = str(Path(base_path) / in_path)
    if not Path(out_path).is_absolute():
        out_path = str(Path(base_path) / out_path)

    if engine == "dask" and _is_excel(in_path):
        raise ValueError("Dask no soporta Excel. Usa CSV/Parquet o ejecuta con --engine pandas.")

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

    # Ejecución
    df_result = run_pipeline(in_path, sheet, forced_header, tol, huge, chunksize)

    write_excel(
        out_path,
        {
            "Resumen": df_result[0],
            "Detalle_por_Serie": df_result[1],
            "Faltantes_intra": df_result[2],
            "Duplicados_por_Serie": df_result[3],
        },
    )
    print(f"✅ OK → Exportado en: {out_path}")


if __name__ == "__main__":
    main()

