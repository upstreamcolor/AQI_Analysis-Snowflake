## AQI Data Pipeline | <img src="https://raw.githubusercontent.com/upstreamcolor/AQI_Analysis-Snowflake/refs/heads/main/icons/snowflake.svg" width="18" /> <img src="https://raw.githubusercontent.com/upstreamcolor/AQI_Analysis-Snowflake/refs/heads/main/icons/streamlit.svg" width="18" /> <img src="https://raw.githubusercontent.com/upstreamcolor/AQI_Analysis-Snowflake/refs/heads/main/icons/githubactions.svg" width="18" />

An end-to-end data pipeline in Snowflake to ingest, process, and visualize hourly AQI data, transforming raw JSON data into structured fact and dimension tables for real-time insights via a Streamlit dashboard.

---

### 🛠️ Deep Down Technical Details

* __Data Ingestion__

    * Hourly AQI data is fetched in JSON format from an external REST API

    * Ingestion is automated and scheduled using GitHub Actions

    * Raw API responses are loaded into a stage, ```RAW_STG``` in Snowflake

* __Bronze Layer (Raw data)__

    * Raw JSON payloads are persisted in a transient table, ```RAW_AQI``` in ```STAGE_SCH``` schema along with ingestion metadata

* __Silver Layer (Cleaned data)__

    * Tasks are used to automate the flattening and transformation of semi-structured JSON data

    * Transformed data is stored in a dynamic table, ```CLEAN_FLATTEN_AQI_DT``` in ```CLEAN_SCH``` schema enabling near-real-time data availability and simplified dependency management

* __Gold Layer (Modeled and Aggregated data)__

    * Curated data is modeled into a star schema for analytics

    * AQI metrics are aggregated at a daily grain and stored in ```AIR_QUALITY_FACT``` fact table in ```CONSUMPTION_SCH``` schema

    * Supporting date (```DIM_DATE```) and location (```DIM_LOCATION```) dimension tables are created in ```CONSUMPTION_SCH``` schema to enable efficient analytical queries

* __Optimization & Maintenance__

    * Dynamic Tables are leveraged to manage data freshness and reduce manual orchestration

* __Data Visualization__

    * Aggregated AQI data is expressed through a Streamlit dashboard

    * The dashboard presents daily AQI trends and location-based air quality insights

---

### 📁 Folder Structure
```
AQI_Analysis-Snowflake/
│
├── icons/                                  
│   ├── githubactions.svg
│   ├── snowflake.svg
│   └── streamlit.svg
├── sql_scripts/                            
│   ├── 01-wh_db_schema.sql               # schema, sarehouses for ingestion, transformation & consumption
│   ├── 02-stg_ff_task.sql                # stage, file format, task & transient table (bronze layer)
│   ├── 03-clean_layer_dt.sql             # dynamic table to store cleaned raw data (silver layer)
│   ├── 04-clean_layer_transpose_dt.sql   # dynamic table to store flattened data (silver layer)
│   ├── 05-wide_table_consumption.sql     # python UDFs, AQI calc. & storage in a wide transformed table
│   ├── 06-fact_and_dim.sql               # location & date dimension tables, AQI fact table (gold layer)
│   └── 07-aggregated_fact_table.sql      # hourly & daily aggregated data (gold layer)
└── README.md                             # project documentation
```
---

