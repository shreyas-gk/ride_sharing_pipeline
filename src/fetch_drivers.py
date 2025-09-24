import pandas as pd
import requests


def fetch_drivers_api():
url = "https://example.com/api/drivers" # Replace with actual API endpoint
response = requests.get(url)
data = response.json()
df = pd.DataFrame(data)
df.to_csv("data/raw/drivers_raw.csv", index=False)
print("Drivers data fetched from API and saved to CSV.")


if __name__ == "__main__":
fetch_drivers_api()