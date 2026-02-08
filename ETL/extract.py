import requests
import pandas as pd
import os

def extract_and_save(base_url, endpoints):

    os.makedirs("raw_data", exist_ok=True)
    extracted_data = {}

    for name, endpoint in endpoints.items():
        url = base_url + endpoint
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception(f"Failed {name}: {response.status_code}")

        json_data = response.json()
        records = json_data.get("data", [])

        if not records:
            print(f"No data found for {name}")
            continue

        df = pd.DataFrame(records)

        # Save raw
        file_path = f"raw_data/{name}.csv"
        df.to_csv(file_path, index=False)

        extracted_data[name] = df

        print(f"Saved {name} to {file_path}")

    return extracted_data
