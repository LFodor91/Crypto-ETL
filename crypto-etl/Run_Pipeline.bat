@echo off
echo Running ETL pipeline...

python src\ingest\fetch_quotes.py
python src\transform\bronze_quotes.py
python src\transform\silver_quotes.py

echo Starting dashboard...
python -m streamlit run src\app\dashboard.py

pause
