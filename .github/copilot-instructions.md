# Copilot / AI Agent Instructions

Purpose: help an AI coding agent become productive quickly in this ETL repo.

- **Big picture**: this is a small end-to-end ETL system. Data flows: extract (Google Sheets) -> transform (pandas) -> load (MySQL). Orchestration is pipeline-based: each pipeline implements `extract`, `transform`, `load`.

- **Key files (examples)**:
  - Pipelines: [pipelines/base_pipeline.py](pipelines/base_pipeline.py) (interface) and [pipelines/daily_record_pipeline.py](pipelines/daily_record_pipeline.py) (concrete)
  - Extract: [extract/google_sheet_extractor.py](extract/google_sheet_extractor.py) (uses `get_gspread_client` in `utils/google_client.py`)
  - Transform utils: [transform/utils_cleaning.py](transform/utils_cleaning.py) (normalization, date parsing, numeric cleaning)
  - Loaders: [loaders/base_sql_loader.py](loaders/base_sql_loader.py) (SQLAlchemy-based bulk insert helper)
  - Config: [config/settings.py](config/settings.py) (env loading, `MYSQL_CONFIG`, `GOOGLE_SHEETS.sheet_index_map`, `DATA_PATHS`)
  - Orchestration entrypoint: [run.py](run.py)

- **Pipeline contract**: follow `extract -> transform -> load`. Transformers return a pandas `DataFrame`; loaders expect a SQLAlchemy `Engine` or use `_bulk_insert` which takes a `DataFrame`. See `ETLPipelineBase` in [pipelines/base_pipeline.py](pipelines/base_pipeline.py).

- **Google Sheets integration**: the extractor authenticates via a service account file path set in env var `GOOGLE_SERVICE_ACCOUNT_FILE` (see `config/settings.py`). Worksheets are referenced by index using `GOOGLE_SHEETS.sheet_index_map`.

- **Database integration**: code expects SQLAlchemy `Engine` (see `loaders/base_sql_loader.py`). However, `requirements.txt` currently lists `mysql-connector-python` but not `SQLAlchemy` or `pymysql`. Agents should verify and, if needed, add `SQLAlchemy` + `pymysql` (or adapt loaders to the installed connector).

- **Environment & run steps**:
  1. Install deps:

     ```bash
     pip install -r requirements.txt
     pip install SQLAlchemy pymysql    # if missing
     ```

  2. Create a `.env` in the repo root (checked by `config/settings.py`) and set at minimum:

     ```text
     DB_HOST=localhost
     DB_PORT=3306
     DB_USER=your_user
     DB_PASSWORD=secret
     DB_NAME=fishfarmsdb
     GOOGLE_SERVICE_ACCOUNT_FILE=/path/to/creds.json
     SPREADSHEET_ID=...
     ```

  3. Run the orchestrator:

     ```bash
     python run.py
     ```

- **Common fixes / gotchas (explicit, actionable)**:
  - `run.py` builds a `db_config` dict but pipelines expect a SQLAlchemy `Engine`. When automating runs, create an engine and pass it to pipelines, for example:

    ```python
    from sqlalchemy import create_engine
    from config.settings import MYSQL_CONFIG

    engine = create_engine(MYSQL_CONFIG.connection_url)
    pipeline = DailyRecordPipeline(sheet_url, engine)
    ```

  - `requirements.txt` likely needs `SQLAlchemy` and `pymysql` (or change loader code to use `mysql-connector` connections). Double-check `loaders/*` imports.
  - Extractor methods sometimes use `print()` for errors; prefer `logging` for consistency and to avoid noisy test output.

- **Conventions & patterns to follow**:
  - Column normalization: call `normalize_columns()` early in transformer chains (see `transform/utils_cleaning.py`).
  - Date parsing: transformers use `clean_date_column()` which handles Excel serials > 40000.
  - Transformers should return `pd.DataFrame` or `None` on failure; pipelines guard failures by logging and returning early.
  - Loaders should use `SQLLoaderBase._bulk_insert()` for bulk writes when possible.

- **What to inspect first when debugging a failing pipeline**:
  1. Confirm `GOOGLE_SERVICE_ACCOUNT_FILE` and spreadsheet URL/ID (see [config/settings.py](config/settings.py) and [run.py](run.py)).
 2. Run the extractor directly (unit test `GoogleSheetExtractor.read_worksheet`) to ensure sheet indexing and header normalization behave as expected.
 3. Validate transformer output shape/types: `normalize_columns`, date columns, numeric columns.
 4. Ensure loader receives a `DataFrame` and that DB `Engine` is created with `MYSQL_CONFIG.connection_url`.

- **Where to add tests / quick local checks**:
  - Add a small test script that calls `GoogleSheetExtractor` with a known sheet and asserts non-empty `DataFrame`.
  - Add unit tests for `transform/utils_cleaning.py` behaviors (Excel serial parsing, normalization).

If anything in these notes is unclear or you want me to merge a specific local `AGENT.md`/`copilot-instructions` draft, tell me what to change and I will iterate.
