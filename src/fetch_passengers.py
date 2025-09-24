import pandas as pd
import requests


def fetch_passengers_api():
url = "https://example.com/api/passengers" # Replace with actual API endpoint
response = requests.get(url)
data = response.json()
df = pd.DataFrame(data)
df.to_csv("data/raw/passengers_raw.csv", index=False)
print("Passengers data fetched from API and saved to CSV.")


if __name__ == "__main__":
fetch_passengers_api()