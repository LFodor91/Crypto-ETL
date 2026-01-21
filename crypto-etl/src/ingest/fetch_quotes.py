import os
import json
import gzip
from datetime import datetime
from pathlib import Path

import requests
from dotenv import load_dotenv


def fetch_latest_quotes(api_key: str, limit: int = 50, convert: str = "USD") -> dict:
    """
    Fetch latest cryptocurrency quotes from CoinMarketCap.
    """
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

    headers = {
        "X-CMC_PRO_API_KEY": api_key,
        "Accept": "application/json",
    }

    params = {
        "limit": limit,
        "convert": convert,
    }

    response = requests.get(url, headers=headers, params=params, timeout=30)
    response.raise_for_status()

    return response.json()


def save_raw_response(data: dict) -> None:
    """
    Save raw API response as gzipped JSON with date/hour partitioning.
    """
    now = datetime.utcnow()
    date_part = now.strftime("%Y-%m-%d")
    hour_part = now.strftime("%H")

    base_path = Path("data/raw/cmc_quotes") / f"date={date_part}" / f"hour={hour_part}"
    base_path.mkdir(parents=True, exist_ok=True)

    file_path = base_path / f"quotes_{now.strftime('%Y%m%dT%H%M%S')}.json.gz"

    with gzip.open(file_path, "wt", encoding="utf-8") as f:
        json.dump(data, f)

    print(f"Saved raw data to: {file_path}")


def main():
    load_dotenv()

    api_key = os.getenv("CMC_API_KEY")
    if not api_key:
        raise RuntimeError("CMC_API_KEY not found in environment variables")

    data = fetch_latest_quotes(api_key=api_key)
    save_raw_response(data)


if __name__ == "__main__":
    main()
