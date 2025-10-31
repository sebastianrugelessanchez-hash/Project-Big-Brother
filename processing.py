# processing.py
import re
from typing import Any  # por si lo necesitas más adelante
import pandas as pd

_EXCEL_ESC = re.compile(r"_x0{0,3}[0-9A-Fa-f]{2,4}_")  # p.ej. _x000A_, _x000d_, _x0009_

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza encabezados: quita \r/\n y códigos Excel _xNNNN_, colapsa espacios, trim."""
    out = df.copy()
    new_cols = []
    for c in out.columns:
        s = str(c)
        # 1) quitar saltos reales
        s = s.replace("\r", " ").replace("\n", " ")
        # 2) des-escapar códigos Excel (_x000a_, _x000d_, _x0009_, etc.)
        s = _EXCEL_ESC.sub(" ", s)
        # 3) colapsar espacios y trim
        s = " ".join(s.split())
        new_cols.append(s)
    out.columns = new_cols
    return out

def coerce_core(df: pd.DataFrame, plant_col: str, ticket_col: str) -> pd.DataFrame:
    """
    Normaliza columnas núcleo antes del pipeline:
    - plant_col -> string limpio
    - ticket_col -> numérico (extrae dígitos)
    - elimina filas sin ticket válido
    - ordena por planta y ticket (estable)
    """
    df = df.copy()

    # Planta como string limpio
    df[plant_col] = df[plant_col].astype(str).str.strip()

    # Ticket: extrae solo dígitos y convierte a número
    ticket_series = (
        df[ticket_col].astype(str).str.extract(r"(\d+)", expand=False)
    )
    df[ticket_col] = pd.to_numeric(ticket_series, errors="coerce")

    # Filtra registros sin ticket y castea a entero
    df = df.dropna(subset=[ticket_col]).reset_index(drop=True)
    df[ticket_col] = df[ticket_col].astype("int64")

    # Orden estable
    df = df.sort_values([plant_col, ticket_col], kind="mergesort").reset_index(drop=True)
    return df


def _norm(s: str) -> str:
    """
    Normaliza encabezados:
    - quita \r y \n
    - pasa a lower
    - colapsa cualquier separador no alfanumérico en espacios
    """
    s = str(s).replace("\r", " ").replace("\n", " ").strip().lower()
    parts = re.split(r"[^a-z0-9]+", s)
    return " ".join(p for p in parts if p)


def match_any(candidates: list[str], df: pd.DataFrame) -> str:
    """
    Devuelve el nombre REAL de la columna que matchee alguno de los candidatos
    tras normalizar. Acepta variaciones tipo 'plant.1', espacios, guiones, etc.
    Estrategia: igualdad -> prefijo -> contiene.
    """
    cand_norm = [_norm(c) for c in candidates]
    norm_cols = {col: _norm(col) for col in df.columns}

    # 1) igualdad exacta
    for col, n in norm_cols.items():
        if n in cand_norm:
            return col

    # 2) empieza por alguno de los candidatos (ej. 'plant.1' -> 'plant')
    for col, n in norm_cols.items():
        if any(n.startswith(c) for c in cand_norm):
            return col

    # 3) contiene (más laxo)
    for col, n in norm_cols.items():
        if any(c in n for c in cand_norm):
            return col

    raise KeyError(f"No se detectó columna para candidatos: {candidates}")


def detect_columns(df: pd.DataFrame):
    """
    Detecta columnas clave (plant/location y ticket).
    Antes de detectar, elimina columnas duplicadas conservando la primera.
    Devuelve: (plant_col, ticket_col)
    """
    # Eliminar columnas duplicadas como 'Plant' y 'Plant.1'
    df = df.loc[:, ~df.columns.duplicated()]

    plant_col  = match_any(['Plant', 'planta', 'location', 'loc', 'site'], df)
    ticket_col = match_any(['Ticket No', 'Ticket', 'Ticket Number', 'Ticket#', 'TicketNo'], df)

    return plant_col, ticket_col

