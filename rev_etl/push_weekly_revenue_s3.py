# rev_etl/push_weekly_revenue_s3.py
import pandas as pd
import boto3
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
s3 = boto3.client('s3',
                  aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))

def push_weekly_revenue_s3():
    df = pd.read_csv("data/marts/fct_trips.csv")
    
    # Compute week start date
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['week'] = df['start_time'].dt.to_period('W').apply(lambda r: r.start_time)
    
    weekly_revenue = df.groupby('week')['fare'].sum().reset_index()
    
    csv_buffer = weekly_revenue.to_csv(index=False)
    s3.put_object(Bucket=os.getenv("S3_BUCKET"),
                  Key=f"reverse_etl/weekly_revenue_{datetime.now().strftime('%Y%m%d')}.csv",
                  Body=csv_buffer)
    print("Weekly revenue pushed to S3.")
