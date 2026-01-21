from pathlib import Path
import duckdb

SILVER_BASE = Path("data/silver/cmc_quotes")


def find_latest_silver_parquet() -> Path:
    files = sorted(SILVER_BASE.rglob("quotes.parquet"))
    if not files:
        raise FileNotFoundError(f"No silver parquet found under: {SILVER_BASE}")
    return files[-1]


def main():
    silver_file = find_latest_silver_parquet()
    print(f"Using silver file: {silver_file}")

    con = duckdb.connect(database=":memory:")

    con.execute(
        f"CREATE VIEW quotes AS SELECT * FROM read_parquet('{silver_file.as_posix()}');"
    )

    print("\n--- Top 10 by market_cap (USD) ---")
    print(
        con.execute("""
            SELECT symbol, name, market_cap, price
            FROM quotes
            WHERE convert = 'USD'
            ORDER BY market_cap DESC
            LIMIT 10
        """).fetchdf()
    )

    print("\n--- Top 10 by volume_24h (USD) ---")
    print(
        con.execute("""
            SELECT symbol, name, volume_24h, price
            FROM quotes
            WHERE convert = 'USD'
            ORDER BY volume_24h DESC
            LIMIT 10
        """).fetchdf()
    )

    print("\n--- Top 10 movers by percent_change_24h (USD) ---")
    print(
        con.execute("""
            SELECT symbol, name, percent_change_24h, price
            FROM quotes
            WHERE convert = 'USD' AND percent_change_24h IS NOT NULL
            ORDER BY percent_change_24h DESC
            LIMIT 10
        """).fetchdf()
    )


if __name__ == "__main__":
    main()
