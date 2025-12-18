# Aquaculture ETL — End-to-End Data Pipeline

A production-oriented ETL pipeline that ingests operational Google Sheets, transforms data with pandas, and loads into MySQL for analytics and forecasting.

## Key features
- Extract: Google Sheets (service account)
- Transform: pandas-based cleaning and normalization
- Load: MySQL via SQLAlchemy (bulk insert helpers)
- Pipeline pattern: each pipeline implements extract → transform → load

## Quick start
1. Create and activate a Python virtualenv.
2. Install dependencies:
   pip install -r requirements.txt
   pip install SQLAlchemy pymysql

3. Create a `.env` in the repo root with at minimum:
   DB_HOST=localhost
   DB_PORT=3306
   DB_USER=your_user
   DB_PASSWORD=secret
   DB_NAME=fishfarmsdb
   GOOGLE_SERVICE_ACCOUNT_FILE=/path/to/creds.json
   SPREADSHEET_ID=...

4. Run the pipeline:
   python run.py

## Environment / Config notes
- Service account file path is read via `GOOGLE_SERVICE_ACCOUNT_FILE`.
- Worksheets are referenced by index using `GOOGLE_SHEETS.sheet_index_map` in `config/settings.py`.
- `run.py` should pass a SQLAlchemy Engine to pipelines. If necessary, create it like:
  from sqlalchemy import create_engine
  engine = create_engine(MYSQL_CONFIG.connection_url)

## Project layout
- pipelines/ — ETL pipeline implementations (base_pipeline.py, daily_record_pipeline.py)
- extract/ — Google Sheets extractor and helpers
- transform/ — data cleaning utilities (normalize columns, date parsing)
- loaders/ — SQLAlchemy-based loaders and bulk insert helpers
- config/ — settings and env loading
- run.py — orchestrator

## Tips & Gotchas
- Ensure `GOOGLE_SERVICE_ACCOUNT_FILE` and `SPREADSHEET_ID` are correct.
- Install `SQLAlchemy` and a MySQL DBAPI (pymysql) if not present.
- Transformers should return a pandas DataFrame; loaders expect a SQLAlchemy Engine or use `_bulk_insert()`.


