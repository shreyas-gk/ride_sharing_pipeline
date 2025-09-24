import pandas as pd
import snowflake.connector
import boto3
import os
from io import StringIO
from dotenv import load_dotenv

load_dotenv()

# AWS S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

# Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

def load_csv_from_s3_to_snowflake(s3_bucket, s3_key, table_name):
    # Download CSV from S3 into memory
    obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    df = pd.read_csv(obj['Body'])
    
    # Optional: do some cleaning or transformations here
    print(f"Loaded {len(df)} rows from s3://{s3_bucket}/{s3_key}")
    
    # Insert into Snowflake
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cols = ','.join(df.columns)
        vals = ','.join([f"'{str(x)}'" for x in row.tolist()])
        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"
        cursor.execute(sql)
    conn.commit()
    cursor.close()
    print(f"Inserted {len(df)} rows into Snowflake table {table_name}")

if __name__ == "__main__":
    # Example usage
    s3_bucket = os.getenv("S3_BUCKET")
    
    load_csv_from_s3_to_snowflake(s3_bucket, "data/raw/trips_raw.csv", "trips_raw")
    load_csv_from_s3_to_snowflake(s3_bucket, "data/raw/drivers_raw.csv", "drivers_raw")
    load_csv_from_s3_to_snowflake(s3_bucket, "data/raw/passengers_raw.csv", "passengers_raw")
