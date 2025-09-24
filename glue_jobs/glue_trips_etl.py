import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

# Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

def glue_trips_etl():
    # Read raw trips CSV
    df = pd.read_csv("data/raw/trips_raw.csv")
    
    # Example transformation: compute trip duration in minutes
    df['trip_duration_mins'] = (pd.to_datetime(df['end_time']) - pd.to_datetime(df['start_time'])).dt.total_seconds()/60
    
    # Load into Snowflake
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cols = ','.join(df.columns)
        vals = ','.join([f"'{str(x)}'" for x in row.tolist()])
        sql = f"INSERT INTO trips_raw ({cols}) VALUES ({vals})"
        cursor.execute(sql)
    conn.commit()
    cursor.close()
    print("Trips ETL job complete.")

if __name__ == "__main__":
    glue_trips_etl()
