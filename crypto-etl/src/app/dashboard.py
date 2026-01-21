from pathlib import Path
from datetime import datetime

import duckdb
import streamlit as st
import pandas as pd
import subprocess
import sys



SILVER_BASE = Path("data/silver/cmc_quotes")


def find_latest_silver_parquet() -> Path:
    files = sorted(SILVER_BASE.rglob("quotes.parquet"))
    if not files:
        raise FileNotFoundError(f"No silver parquet found under: {SILVER_BASE}")
    return files[-1]

def run_pipeline():
    """
    Runs the ETL pipeline: ingest -> bronze -> silver
    """
    commands = [
        [sys.executable, "src/ingest/fetch_quotes.py"],
        [sys.executable, "src/transform/bronze_quotes.py"],
        [sys.executable, "src/transform/silver_quotes.py"],
    ]

    for cmd in commands:
        subprocess.run(cmd, check=True)


def list_silver_partitions():
    files = sorted(SILVER_BASE.rglob("quotes.parquet"))
    # show newest first
    return list(reversed(files))


def load_tables(parquet_path: Path):
    con = duckdb.connect(database=":memory:")
    con.execute(f"CREATE VIEW quotes AS SELECT * FROM read_parquet('{parquet_path.as_posix()}');")

    top_mcap = con.execute("""
        SELECT symbol, name, market_cap, price
        FROM quotes
        WHERE convert = 'USD'
        ORDER BY market_cap DESC
        LIMIT 10
    """).fetchdf()

    top_vol = con.execute("""
        SELECT symbol, name, volume_24h, price
        FROM quotes
        WHERE convert = 'USD'
        ORDER BY volume_24h DESC
        LIMIT 10
    """).fetchdf()

    movers = con.execute("""
        SELECT symbol, name, percent_change_24h, price
        FROM quotes
        WHERE convert = 'USD' AND percent_change_24h IS NOT NULL
        ORDER BY percent_change_24h DESC
        LIMIT 10
    """).fetchdf()

    freshness = con.execute("""
        SELECT MAX(last_updated) AS max_last_updated, COUNT(*) AS rows
        FROM quotes
        WHERE convert='USD'
    """).fetchdf()

    return top_mcap, top_vol, movers, freshness


st.set_page_config(page_title="Crypto ETL Dashboard", layout="wide")
st.title("📈 Crypto ETL Dashboard (CoinMarketCap)")
st.caption("Raw → Bronze → Silver (Parquet) → DuckDB SQL → Dashboard")

# Sidebar selection
st.sidebar.header("Dataset")
st.sidebar.divider()

if st.sidebar.button("🔄 Refresh data"):
    with st.spinner("Running ETL pipeline..."):
        run_pipeline()
    st.success("Pipeline finished successfully!")
    st.rerun()


available = list_silver_partitions()
if not available:
    st.error("No silver parquet files found. Run ingest → bronze → silver first.")
    st.stop()

default = 0
choice = st.sidebar.selectbox(
    "Select partition (latest first)",
    options=list(range(len(available))),
    format_func=lambda i: str(available[i]),
    index=default
)
parquet_path = available[choice]

st.sidebar.write("Selected file:")
st.sidebar.code(str(parquet_path))

# Load data
top_mcap, top_vol, movers, freshness = load_tables(parquet_path)

# Header KPIs
col1, col2, col3 = st.columns(3)
max_ts = freshness["max_last_updated"].iloc[0]
rows = int(freshness["rows"].iloc[0])

col1.metric("Rows (USD)", f"{rows}")
col2.metric("Latest 'last_updated'", str(max_ts))

# parse partition date/hour from path (nice-to-have)
date_part = next((p.split("=", 1)[1] for p in parquet_path.parts if p.startswith("date=")), None)
hour_part = next((p.split("=", 1)[1] for p in parquet_path.parts if p.startswith("hour=")), None)
col3.metric("Partition", f"{date_part} / hour={hour_part}")

st.divider()

# Tables + charts
left, right = st.columns(2)

with left:
    st.subheader("🏆 Top 10 by Market Cap (USD)")
    st.dataframe(top_mcap, use_container_width=True)

    st.subheader("📊 Market Cap chart")
    chart_df = top_mcap[["symbol", "market_cap"]].set_index("symbol")
    st.bar_chart(chart_df)

with right:
    st.subheader("💧 Top 10 by Volume 24h (USD)")
    st.dataframe(top_vol, use_container_width=True)

    st.subheader("🚀 Top 10 Movers (24h % change, USD)")
    st.dataframe(movers, use_container_width=True)

    movers_chart = movers[["symbol", "percent_change_24h"]].set_index("symbol")
    st.bar_chart(movers_chart)

st.caption("Tip: run the ingest again to create new date/hour partitions and switch them from the sidebar.")
