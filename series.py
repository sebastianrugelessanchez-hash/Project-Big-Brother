"""
Lógica de series, faltantes intra, duplicados e "huge jumps" inter-series.
"""
from __future__ import annotations
from typing import Tuple, Dict
import pandas as pd
import numpy as np


def ranges_from_starts_ends(starts: np.ndarray, ends: np.ndarray) -> str:
    parts = []
    for a, b in zip(starts, ends):
        parts.append(str(int(a)) if a == b else f"{int(a)}-{int(b)}")
    return ", ".join(parts)


def count_from_ranges_str(ranges_str: str) -> int:
    if ranges_str is None or (isinstance(ranges_str, float) and pd.isna(ranges_str)):
        return 0
    s = str(ranges_str).strip()
    if not s:
        return 0
    total = 0
    for p in (x.strip() for x in s.split(",") if x.strip()):
        if "-" in p:
            a, b = p.split("-", 1)
            try:
                a, b = int(a), int(b)
            except ValueError:
                continue
            if b >= a:
                total += (b - a + 1)
        else:
            try:
                int(p)
                total += 1
            except ValueError:
                continue
    return int(total)


def detect_series(df_core: pd.DataFrame, plant_col: str, ticket_col: str, tolerance: int) -> pd.DataFrame:
    """Asigna serie_id por planta cuando salta más que (1 + tolerance)."""
    diff = df_core.groupby(plant_col, observed=True)[ticket_col].diff()
    new_block = diff.gt(1 + tolerance)
    df_core = df_core.copy()
    df_core['series_id'] = new_block.groupby(df_core[plant_col]).cumsum().fillna(0).astype('Int64')
    return df_core


def aggregate_by_series(df_core: pd.DataFrame, plant_col: str, ticket_col: str) -> pd.DataFrame:
    g = df_core.groupby([plant_col, 'series_id'], observed=True)[ticket_col]
    agg = g.agg(['count', 'min', 'max']).reset_index().rename(columns={'count':'Count', 'min':'Min', 'max':'Max'})
    return agg.sort_values([plant_col, 'series_id']).reset_index(drop=True)


def compute_inter_series_gaps(agg_series: pd.DataFrame, plant_col: str, huge_jump_threshold: int) -> pd.DataFrame:
    df = agg_series.sort_values([plant_col, 'series_id']).copy()
    df['Next_Min'] = df.groupby(plant_col, observed=True)['Min'].shift(-1)
    df['Curr_Max'] = df['Max']
    df['Inter_Len'] = (df['Next_Min'] - df['Curr_Max'] - 1)

    def fmt_range(a, b):
        if pd.isna(a) or pd.isna(b) or (b < a):
            return ""
        return f"{int(a)}-{int(b)}" if a != b else str(int(a))

    df['Inter_Range'] = df.apply(
        lambda r: fmt_range(r['Curr_Max'] + 1, r['Next_Min'] - 1) if pd.notna(r['Next_Min']) else "", axis=1
    )
    df['Huge Jump'] = df['Inter_Len'].where(df['Inter_Len'] >= huge_jump_threshold, np.nan)
    df['Huge Jump'] = df['Huge Jump'].combine_first(pd.Series([np.nan]*len(df)))
    df['Huge Jump'] = df.apply(
        lambda r: r['Inter_Range'] if pd.notna(r['Huge Jump']) and r['Inter_Range'] else np.nan, axis=1
    )
    return df[[plant_col, 'series_id', 'Min', 'Max', 'Inter_Len', 'Huge Jump']]


def intra_missing_ranges(s: pd.Series, intra_tolerance: int = 0) -> pd.Series:
    vals = s.dropna().astype(np.int64).to_numpy()
    vals = np.sort(vals)
    if vals.size <= 1:
        return pd.Series({"Tickets Involucrados": "", "Tickets_missing_serie": 0})
    dif = np.diff(vals)
    mask = dif > (1 + intra_tolerance)  # Usar tolerancia específica
    if not np.any(mask):
        return pd.Series({"Tickets Involucrados": "", "Tickets_missing_serie": 0})
    starts = vals[:-1][mask] + 1
    ends   = vals[1:][mask]  - 1
    all_str = ranges_from_starts_ends(starts, ends)
    missing_total = count_from_ranges_str(all_str)
    return pd.Series({"Tickets Involucrados": all_str, "Tickets_missing_serie": missing_total})


def duplicates_by_series(df_core: pd.DataFrame, plant_col: str, ticket_col: str) -> pd.DataFrame:
    counts = (
        df_core.groupby([plant_col, 'series_id', ticket_col], observed=True)
               .size()
               .reset_index(name='freq')
    )
    dups = counts[counts['freq'] > 1]
    # Compact ranges por grupo planta/serie
    def _pack(g: pd.DataFrame) -> pd.Series:
        vals = g[ticket_col].dropna().astype(np.int64).to_numpy()
        vals = np.unique(vals)
        if vals.size == 0:
            return pd.Series({"Tickets Involucrados": "", "Huge Jump": ""})
        starts = [vals[0]]; ends = [vals[0]]
        for v in vals[1:]:
            if v == ends[-1] + 1:
                ends[-1] = v
            else:
                starts.append(v); ends.append(v)
        return pd.Series({"Tickets Involucrados": ranges_from_starts_ends(np.array(starts), np.array(ends))})
    if dups.empty:
        return pd.DataFrame(columns=[plant_col, 'series_id', 'Tickets Involucrados', 'freq'])
    packed = dups.groupby([plant_col, 'series_id']).apply(_pack, include_groups=False).reset_index()
    # Frecuencia total duplicada
    total_freq = dups.groupby([plant_col, 'series_id'], observed=True)['freq'].sum().reset_index(name='freq')
    return packed.merge(total_freq, on=[plant_col, 'series_id'], how='left')


def summarize_by_plant(agg_series: pd.DataFrame, inter_series: pd.DataFrame, plant_col: str) -> pd.DataFrame:
    # Resumen por planta: suma de Count, min global y max global; concatenar Huge Jump
    inter_by_plant = (inter_series[[plant_col, 'Huge Jump']]
                      .dropna()
                      .groupby(plant_col, observed=True)['Huge Jump']
                      .apply(lambda s: ", ".join([x for x in s if isinstance(x, str) and x.strip()]))  # concat
                      .reset_index())
    resumen = (
        agg_series.groupby(plant_col, observed=True)
                  .agg(Count=('Count','sum'),
                       Min=('Min','min'),
                       Max=('Max','max'))
                  .reset_index()
                  .merge(inter_by_plant, on=plant_col, how='left')
    )
    if 'Huge Jump' in resumen.columns:
        resumen['Count_Huge_Jump'] = resumen['Huge Jump'].fillna("").apply(count_from_ranges_str).astype('Int64')
        cols = list(resumen.columns)
        hj_idx = cols.index('Huge Jump')
        resumen = resumen[cols[:hj_idx+1] + ['Count_Huge_Jump'] +
                          [c for i, c in enumerate(cols) if i > hj_idx and c != 'Count_Huge_Jump']]
    return resumen
