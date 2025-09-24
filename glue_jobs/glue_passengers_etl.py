import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

def glue_passengers_etl():
    df = pd.read_csv("data/raw/passengers_raw.csv")
    
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cols = ','.join(df.columns)
        vals = ','.join([f"'{str(x)}'" for x in row.tolist()])
        sql = f"INSERT INTO passengers_raw ({cols}) VALUES ({vals})"
        cursor.execute(sql)
    conn.commit()
    cursor.close()
    print("Passengers ETL job complete.")

if __name__ == "__main__":
    glue_passengers_etl()
