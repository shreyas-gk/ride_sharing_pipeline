# rev_etl/push_top_drivers_s3.py
import pandas as pd
import boto3
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
s3 = boto3.client('s3',
                  aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))

def push_top_drivers_s3():
    df = pd.read_csv("data/marts/dim_driver.csv")
    
    # Performance score: weighted metric
    df['performance_score'] = df['avg_trip_rating'] * 0.6 + df['total_trips'] * 0.4
    
    top_drivers = df.sort_values('performance_score', ascending=False).head(20)
    
    csv_buffer = top_drivers.to_csv(index=False)
    s3.put_object(Bucket=os.getenv("S3_BUCKET"),
                  Key=f"reverse_etl/top_drivers_{datetime.now().strftime('%Y%m%d')}.csv",
                  Body=csv_buffer)
    print("Top drivers pushed to S3.")
