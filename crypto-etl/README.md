# \# Crypto Market Data ETL Pipeline (CoinMarketCap)

# 

# \## Overview

# This project demonstrates an end-to-end \*\*data engineering ETL pipeline\*\*

# built on real cryptocurrency market data from the CoinMarketCap API.

# 

# The pipeline ingests semi-structured JSON data, transforms it into analytical

# Parquet datasets, and exposes the results through an interactive dashboard.

# 

# The project is designed for \*\*learning and interview demonstration purposes\*\*.

# 

# ---

# 

# \## Architecture

# 

# CoinMarketCap API  

# → Raw layer (JSON, immutable, time-partitioned)  

# → Bronze layer (normalized Parquet)  

# → Silver layer (cleaned \& deduplicated Parquet)  

# → DuckDB (SQL analytics)  

# → Streamlit Dashboard  

# 

# ---

# 

# \## Data Layers

# 

# \### Raw layer

# \- Source: CoinMarketCap `listings/latest` API

# \- Format: gzipped JSON

# \- Storage: `data/raw/cmc\_quotes/date=YYYY-MM-DD/hour=HH/`

# \- Characteristics:

# &nbsp; - Immutable

# &nbsp; - Time-partitioned

# &nbsp; - One file per ingestion run

# 

# \### Bronze layer

# \- Format: Parquet

# \- Structure:

# &nbsp; - One row per asset per currency (USD)

# &nbsp; - Nested JSON (`quote.USD`) flattened

# \- Storage: `data/bronze/cmc\_quotes/date=YYYY-MM-DD/hour=HH/`

# \- Purpose:

# &nbsp; - Schema normalization

# &nbsp; - Minimal transformations

# 

# \### Silver layer

# \- Format: Parquet

# \- Transformations:

# &nbsp; - Deduplication on `(asset\_id, last\_updated, convert)`

# &nbsp; - Basic data quality checks

# &nbsp; - Removal of invalid values

# \- Storage: `data/silver/cmc\_quotes/date=YYYY-MM-DD/hour=HH/`

# \- Purpose:

# &nbsp; - Analytics-ready dataset

# 

# ---

# 

# \## Analytics

# Analytics are performed using \*\*DuckDB\*\*, querying Parquet files directly.

# 

# Example queries:

# \- Top cryptocurrencies by market capitalization

# \- Top cryptocurrencies by 24h trading volume

# \- Top 24h price movers

# 

# DuckDB allows fast, in-process SQL analytics without requiring a separate

# database engine.

# 

# ---

# 

# \## Dashboard

# A \*\*Streamlit\*\* dashboard provides a user-friendly interface to explore

# the silver layer.

# 

# Features:

# \- Partition selector (date/hour)

# \- Key metrics (row count, freshness)

# \- Top 10 tables

# \- Bar charts for market cap and price movers

# 

# The dashboard runs locally in a browser.

# 

# ---

# 

# \## Tech Stack

# \- Python

# \- CoinMarketCap API

# \- pandas

# \- Parquet / PyArrow

# \- DuckDB

# \- Streamlit

# \- Git

# 

# ---

# 

# \## Project Structure

# 



