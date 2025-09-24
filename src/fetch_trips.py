import pandas as pd
import requests


def fetch_trips_api():
url = "https://example.com/api/trips" # Replace with actual API endpoint
response = requests.get(url)
data = response.json()
df = pd.DataFrame(data)
df.to_csv("data/raw/trips_raw.csv", index=False)
print("Trips data fetched from API and saved to CSV.")


if __name__ == "__main__":
fetch_trips_api()