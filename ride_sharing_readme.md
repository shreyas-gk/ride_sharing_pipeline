# Ride-Sharing Data Engineering Pipeline

## **Project Overview**
This project simulates a **production-grade ride-sharing data pipeline**. It covers the full **ELT lifecycle**, from raw data ingestion to analytics-ready marts and Reverse ETL outputs.  

**Key components:**
- Raw ride-sharing data (drivers, passengers, trips) stored in **S3**  
- **AWS Glue** ETL scripts for ingestion and transformation  
- **Snowflake** as the data warehouse  
- **dbt models** for staging and marts (enhanced with derived metrics)  
- **Airflow DAG** for orchestration  
- **Reverse ETL** to push top drivers, top passengers, and weekly revenue aggregates to S3  
- **dbt tests** to ensure data quality  

---

## **Folder Structure**

```
project_root/
├─ airflow_home/          # Airflow configs & DAGs
├─ data/
│  └─ raw/               # Raw CSVs (drivers, passengers, trips)
├─ glue_jobs/             # Glue ETL scripts
├─ models/
│  ├─ staging/           # dbt staging models
│  └─ marts/             # dbt marts with enhanced metrics
├─ rev_etl/               # Reverse ETL scripts
├─ tests/                 # dbt / custom SQL tests
├─ logs/                  # Airflow logs
├─ .gitignore
├─ dbt_project.yml
├─ requirements.txt
└─ readme.md
```

---

## **Setup Instructions**

### **1. Clone the Repo**
```bash
git clone <repo-url>
cd project_root
```

### **2. Python Environment**
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
pip install -r requirements.txt
```

### **3. Environment Variables**
Create a `.env` file with:

```
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
S3_BUCKET=your_s3_bucket_name
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema
```

---

## **4. Airflow DAG**
- DAG handles:
  - Raw data ingestion from S3  
  - Glue ETL scripts to load into Snowflake  
  - dbt transformations (staging + marts)  
  - Reverse ETL tasks to push analytics outputs to S3  

Start Airflow locally:

```bash
export AIRFLOW_HOME=airflow_home
airflow db init
airflow scheduler
airflow webserver
```

Access the Airflow UI at `http://localhost:8080` and trigger the DAG.

---

## **5. dbt Models**
- **Staging models:** clean and standardize raw data  
- **Marts:**  
  - `dim_driver` & `dim_passenger` with aggregates (total trips, avg fares, ratings)  
  - `fct_trips` enriched with derived metrics (trip duration, avg speed, fare per km, is_long_trip)  

Run dbt:

```bash
dbt deps
dbt run
dbt test
```

---

## **6. Reverse ETL**
- Pushes analytics outputs to S3:
  - Top drivers by performance score  
  - Top passengers by engagement score  
  - Weekly revenue aggregates  

Run manually or via Airflow DAG:

```bash
python rev_etl/push_top_drivers_s3.py
python rev_etl/push_top_passengers_s3.py
python rev_etl/push_weekly_revenue_s3.py
```

---

## **7. Data Quality & Tests**
- dbt built-in tests: `not_null`, `unique`, `relationships`  
- Custom SQL tests for invalid fares, distances, or ratings  

---

## **8. Project Highlights**
- Fully **local setup with Ubuntu + VSCode** compatibility  
- **Glue scripts** simulate AWS ETL jobs  
- **Reverse ETL** demonstrates operational analytics workflow  
- **Production-grade marts** with enriched metrics for BI / dashboards  
- **Extensible**: Add new dimensions, real-time processing, or ML analytics

