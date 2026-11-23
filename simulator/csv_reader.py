import math
from typing import Dict, Iterator, Optional, Any
import pandas as pd

EXPECTED_COLUMNS = [
    "timestamp",
    "V",
    "A",
    "P_agg",
    "ac_1",
    "kettle",
    "boiler",
    "fridge",
    "washing_machine",
    "issues",
]

FLOAT_COLUMNS = ["V", "A", "P_agg", "ac_1", "kettle", "boiler", "fridge", "washing_machine"]


def _clean_value(val: Any) -> Optional[float]:
    if val is None or (isinstance(val, float) and math.isnan(val)) or pd.isna(val):
        return None
    return float(val)


def read_energy_records(csv_path: str, max_rows: int = 0) -> Iterator[Dict[str, Any]]:
    nrows = max_rows if max_rows and max_rows > 0 else None
    df = pd.read_csv(csv_path, nrows=nrows)

    missing_cols = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"CSV is missing expected columns: {missing_cols}")

    for row in df[EXPECTED_COLUMNS].to_dict(orient="records"):
        record: Dict[str, Any] = {
            "timestamp": str(row["timestamp"]),
            "issues": int(row["issues"]),
        }
        for col in FLOAT_COLUMNS:
            record[col] = _clean_value(row[col])
        yield record
