from pathlib import Path
import pandas as pd


BRONZE_BASE = Path("data/bronze/cmc_quotes")
SILVER_BASE = Path("data/silver/cmc_quotes")


def find_latest_bronze_parquet() -> Path:
    files = sorted(BRONZE_BASE.rglob("quotes.parquet"))
    if not files:
        raise FileNotFoundError(f"No bronze parquet found under: {BRONZE_BASE}")
    return files[-1]


def parse_partition_from_path(p: Path) -> tuple[str, str]:
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


def clean_and_dedup(df: pd.DataFrame) -> pd.DataFrame:
    # Ensure required columns exist
    required = ["asset_id", "last_updated", "convert"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Missing required column: {c}")

    # Drop rows with missing keys
    df = df.dropna(subset=["asset_id", "last_updated", "convert"])

    # Basic sanity: non-negative metrics (allow NaN)
    nonneg_cols = ["price", "volume_24h", "market_cap"]
    for c in nonneg_cols:
        if c in df.columns:
            df = df[(df[c].isna()) | (df[c] >= 0)]

    # Deduplicate: keep the last occurrence (stable enough for demo)
    df = df.sort_values(["asset_id", "convert", "last_updated"])
    df = df.drop_duplicates(subset=["asset_id", "last_updated", "convert"], keep="last")

    return df.reset_index(drop=True)


def write_silver(df: pd.DataFrame, date_part: str, hour_part: str) -> Path:
    out_dir = SILVER_BASE / f"date={date_part}" / f"hour={hour_part}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "quotes.parquet"
    df.to_parquet(out_file, index=False)
    return out_file


def main():
    bronze_file = find_latest_bronze_parquet()
    date_part, hour_part = parse_partition_from_path(bronze_file)

    df = pd.read_parquet(bronze_file)
    before = len(df)
    df2 = clean_and_dedup(df)
    after = len(df2)

    out_file = write_silver(df2, date_part, hour_part)

    print(f"Bronze in:  {bronze_file}")
    print(f"Rows in:    {before}")
    print(f"Rows out:   {after}")
    print(f"Silver out: {out_file}")


if __name__ == "__main__":
    main()
