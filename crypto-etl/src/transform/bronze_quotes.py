import json
import gzip
from pathlib import Path
from datetime import datetime

import pandas as pd


RAW_BASE = Path("data/raw/cmc_quotes")
BRONZE_BASE = Path("data/bronze/cmc_quotes")


def find_latest_raw_file() -> Path:
    files = sorted(RAW_BASE.rglob("quotes_*.json.gz"))
    if not files:
        raise FileNotFoundError(f"No raw files found under: {RAW_BASE}")
    return files[-1]


def parse_partition_from_path(p: Path) -> tuple[str, str]:
    # expects .../date=YYYY-MM-DD/hour=HH/quotes_xxx.json.gz
    date_part = None
    hour_part = None
    for part in p.parts:
        if part.startswith("date="):
            date_part = part.split("=", 1)[1]
        if part.startswith("hour="):
            hour_part = part.split("=", 1)[1]
    if not date_part or not hour_part:
        raise ValueError(f"Could not parse date/hour partitions from path: {p}")
    return date_part, hour_part


def load_raw_json_gz(path: Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)


def flatten_quotes(payload: dict) -> pd.DataFrame:
    """
    CMC listings/latest response shape (simplified):
      { "status": {...}, "data": [ {id, name, symbol, slug, num_market_pairs, ...,
                                   "quote": {"USD": {...}, "EUR": {...}} } ] }
    We'll normalize into one row per asset per convert currency.
    """
    ingest_ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"

    rows = []
    data = payload.get("data", [])
    for asset in data:
        asset_id = asset.get("id")
        symbol = asset.get("symbol")
        name = asset.get("name")
        slug = asset.get("slug")

        last_updated = asset.get("last_updated")  # usually ISO string

        # quote is a dict keyed by currency: "USD", "EUR", ...
        quote = asset.get("quote", {}) or {}
        for convert, q in quote.items():
            rows.append(
                {
                    "asset_id": asset_id,
                    "symbol": symbol,
                    "name": name,
                    "slug": slug,
                    "last_updated": last_updated,
                    "convert": convert,

                    "price": q.get("price"),
                    "volume_24h": q.get("volume_24h"),
                    "market_cap": q.get("market_cap"),
                    "percent_change_1h": q.get("percent_change_1h"),
                    "percent_change_24h": q.get("percent_change_24h"),
                    "percent_change_7d": q.get("percent_change_7d"),

                    "raw_ingest_ts": ingest_ts,
                }
            )

    df = pd.DataFrame(rows)

    # Basic typing (safe)
    numeric_cols = [
        "price", "volume_24h", "market_cap",
        "percent_change_1h", "percent_change_24h", "percent_change_7d",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def write_bronze(df: pd.DataFrame, date_part: str, hour_part: str) -> Path:
    out_dir = BRONZE_BASE / f"date={date_part}" / f"hour={hour_part}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_file = out_dir / "quotes.parquet"
    df.to_parquet(out_file, index=False)
    return out_file


def main():
    latest_file = find_latest_raw_file()
    date_part, hour_part = parse_partition_from_path(latest_file)

    payload = load_raw_json_gz(latest_file)
    df = flatten_quotes(payload)

    out_file = write_bronze(df, date_part, hour_part)

    print(f"Raw file:   {latest_file}")
    print(f"Rows:       {len(df)}")
    print(f"Bronze out: {out_file}")


if __name__ == "__main__":
    main()

