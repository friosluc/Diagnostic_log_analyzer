from pathlib import Path
import pandas as pd

REQUIRED_COLUMNS = {"timestamp", "signal", "value"}

def load_log(csv_path: str | Path) -> pd.DataFrame:
    csv_path = Path(csv_path)

    df = pd.read_csv(csv_path)

    # 1) Validar columnas requeridas
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # 2) Convertir timestamp a datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    if df["timestamp"].isna().any():
        bad_rows = df[df["timestamp"].isna()]
        raise ValueError(f"Invalid timestamp in {len(bad_rows)} row(s)")

    # 3) Convertir value a número (si no se puede, queda NaN)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df