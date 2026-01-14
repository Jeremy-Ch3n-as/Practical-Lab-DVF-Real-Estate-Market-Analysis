# 🏡 Lab 1 : Real Estate Data Pipeline (DVF 2026)

Student: Jérémy Chen
Course: ECE 2026 - Data Integration

## 📋 Description
Technical and analytical study of the French real estate market (January 2026) using a local Data Lake and Data Warehouse architecture without cloud services.

## 🏗️ Architecture
The project follows a strict "Local Data Lake" structure:
- `data_lake/raw/`: Original immutable files (Source: data.gouv.fr).
- `data_lake/staging/`: Cleaned and standardized CSVs.
- `data_lake/curated/`: BI-ready datasets (Aggregated trends).
- `warehouse/`: DuckDB local database (`real_estate.duckdb`).
- `scripts/`: Python ETL scripts.

## 🛠️ Tools Used
- **Python**: For data ingestion and transformation.
- **DuckDB**: As a local OLAP Data Warehouse.
- **Pandas**: For data manipulation.
