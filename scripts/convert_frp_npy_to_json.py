#!/usr/bin/env python3
"""
Convert FRP time series stored as pickled numpy arrays (.npy) into JSON.

Each .npy is expected to contain a 2xN object array:
  row 0: pandas timestamps
  row 1: FRP values in MW

Output JSON (one per input) is written to FRP_JSON/<stem>.json
with records sorted by time:
  [{ "t": "ISO8601Z", "frp": <float or null> }, ...]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
IN_DIR = ROOT / "FRP"
OUT_DIR = ROOT / "FRP_JSON"


def convert_file(path: Path) -> str:
    try:
        arr = np.load(path, allow_pickle=True)
    except Exception as exc:
        return f"skip {path.name}: {exc}"

    if arr.shape[0] != 2:
        return f"skip {path.name}: unexpected shape {arr.shape}"

    ts = pd.Series(arr[0])
    frp = pd.Series(arr[1])
    df = pd.DataFrame({"t": ts, "frp": frp}).sort_values("t")

    records: List[dict] = []
    for r in df.itertuples():
        try:
            t_iso = pd.to_datetime(r.t).to_pydatetime().isoformat().replace("+00:00", "Z")
        except Exception:
            continue
        val = float(r.frp) if pd.notnull(r.frp) else None
        records.append({"t": t_iso, "frp": val})

    out_path = OUT_DIR / f"{path.stem}.json"
    out_path.write_text(json.dumps(records, separators=(",", ":")))
    return f"wrote {out_path.name} ({len(records)} points)"


def main() -> None:
    if not IN_DIR.is_dir():
        raise SystemExit(f"Input dir not found: {IN_DIR}")
    OUT_DIR.mkdir(exist_ok=True)

    messages = []
    for npy in sorted(IN_DIR.glob("*.npy")):
        msg = convert_file(npy)
        if msg:
            messages.append(msg)

    for msg in messages:
        print(msg)


if __name__ == "__main__":
    main()
