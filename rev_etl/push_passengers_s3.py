# rev_etl/push_top_passengers_s3.py
import pandas as pd
import boto3
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
s3 = boto3.client('s3',
                  aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                  aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))

def push_top_passengers_s3():
    df = pd.read_csv("data/marts/dim_passenger.csv")
    
    # Engagement score: total trips * avg fare paid
    df['engagement_score'] = df['total_trips'] * df['avg_fare_paid']
    
    top_passengers = df.sort_values('engagement_score', ascending=False).head(20)
    
    csv_buffer = top_passengers.to_csv(index=False)
    s3.put_object(Bucket=os.getenv("S3_BUCKET"),
                  Key=f"reverse_etl/top_passengers_{datetime.now().strftime('%Y%m%d')}.csv",
                  Body=csv_buffer)
    print("Top passengers pushed to S3.")
