# Contributing to VoltStream

Thank you for your interest in contributing to the VoltStream EV Grid Resilience Pipeline. This document outlines how to get started, how the project is structured, and the standards we follow to keep the codebase clean and production-grade.

## Getting Started

Before contributing, make sure you can run the full pipeline locally. Follow the [Local Setup Guide](docs/02_local_setup.md) to get everything working end to end. If you run into issues during setup, open an issue rather than a pull request — it helps us improve the documentation for everyone.

## How to Contribute

### Reporting a bug

If you find a bug, open a GitHub Issue with the following information:

- A clear description of what went wrong
- Which script or dbt model is affected
- The exact error message or log output
- The steps to reproduce it
- Your environment — OS, Python version, dbt version

### Suggesting an improvement

Open a GitHub Issue with the label `enhancement`. Describe what you want to change and why. For significant changes — new pipeline stages, new data sources, schema changes — discuss the approach in the issue before opening a pull request.

### Submitting a pull request

1. Fork the repository and create a branch from `main`:

```bash
git checkout -b feature/your-feature-name
```

2. Make your changes following the code standards below
3. Test your changes locally — run the pipeline end to end and confirm all dbt tests pass
4. Push your branch and open a pull request against `main`
5. Describe what you changed and why in the pull request description

## Code Standards

### Python

All Python scripts follow these conventions:

- Use `logging` not `print` for any output. Every script has a logger configured at the top using `logging.basicConfig` with timestamps and log levels
- Use type hints on all function signatures
- Functions have docstrings explaining what they do, what they return, and any important behaviour to know
- Secrets come from environment variables only — never hardcoded
- Error handling uses try/except with specific exception types, not bare `except:`
- S3 reads use paginators — never `list_objects_v2` directly, which silently truncates at 1000 objects

### dbt

- All models have a description in `schema.yml`
- All key columns have at least a `not_null` test
- Primary keys have both `not_null` and `unique` tests
- Business logic lives in Gold (`marts/`) models — staging models are clean selects only
- No hardcoded credentials or environment-specific values in SQL files — use `{{ var() }}` for configurable values
- Model names are lowercase with underscores — `dim_station`, `fct_risk`, `fact_station_status`

### SQL

- All SQL is written for Redshift compatibility — avoid PostgreSQL-specific syntax like `QUALIFY` or `ctid`
- Use CTEs rather than subqueries for readability
- Column aliases are lowercase with underscores
- TRUNCATE always precedes COPY — never append without clearing first

### Git

- Branch names use the format `feature/description`, `fix/description`, or `docs/description`
- Commit messages are concise and describe what changed, not why — the pull request description explains the why
- Never commit `.env`, `profiles.yml` with real credentials, `target/`, `dbt_packages/`, or `logs/`
- The `.gitignore` already covers these — double check with `git status` before committing

## Project Architecture Decisions

These are decisions that were made intentionally. If you want to change them, open an issue to discuss first.

**Why Pandas instead of PySpark in processing scripts?**
The current dataset (5000 stations, 89 weather zones) fits comfortably in memory. Pandas keeps the processing scripts simple and portable — no Spark cluster needed. If the dataset grows beyond ~500k rows, migration to PySpark on EMR is the natural next step.

**Why is weather ingestion dynamic rather than using hardcoded zones?**
`weather_ingest.py` reads distinct `lat_round`/`lon_round` pairs from Silver EV data in S3. This means weather coverage automatically expands as new stations are ingested — no manual updates needed. Hardcoded zones would drift out of sync with the actual station footprint over time.

**Why TRUNCATE + COPY instead of upsert/merge in Redshift?**
The `public` schema is a landing zone, not the analytical layer. dbt handles deduplication and history tracking in the Gold models. Keeping the landing zone simple — wipe and reload on every run — makes it easy to reason about and eliminates merge complexity at the COPY layer.

**Why is the Metabase dashboard not automated?**
Dashboard layout and visualisation configuration are stored in Metabase's internal database, not as code. Automating this would require either the Metabase API (available on paid plans) or a dashboard-as-code tool like Lightdash or Evidence. For now the dashboard is documented in the README and reproduced manually. Contributions that automate this are welcome.

## Areas Open for Contribution

These are improvements that would add real value to the project:

- **Incremental Silver processing** — `bronze_to_silver_ev.py` currently reads all Bronze files on every run. Adding watermark-based incremental reads would reduce processing time significantly as the dataset grows
- **PySpark migration** — replacing Pandas with PySpark in the processing scripts for scalability
- **dbt snapshots** — replacing the manual SCD Type 2 columns with dbt's built-in snapshot functionality
- **Airflow alerting** — adding Slack or email notifications when DAG runs fail
- **Data quality quarantine** — routing records with `data_quality_error = true` to a separate quarantine table for investigation rather than just flagging them
- **Additional data sources** — integrating grid load data or EV usage patterns to enrich the risk model
- **Dashboard automation** — scripting the Metabase dashboard setup using the Metabase API

## Questions

If something in the codebase is unclear or undocumented, open an issue with the label `question`. Good questions often lead to better documentation.
