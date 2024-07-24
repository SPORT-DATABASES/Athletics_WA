import requests
import logging
import time
from datetime import datetime
from tqdm import tqdm

# Endpoint URL and headers
url = "https://graphql-prod-4609.prod.aws.worldathletics.org/graphql"
headers = {
    "Content-Type": "application/json",
    "x-api-key": "da2-4enkl5vjkbczbfowagsi5l4qpy"
}

# Constants
RETRIES = 3
TIMEOUT = 30
LIMIT = 200

# Get today's date in the required format
today_date = datetime.today().strftime('%Y-%m-%d')

# Get the total number of competitions (replace this with the actual total if known)
total_competitions = 1000  # Example total, replace with actual number if known

# Fetch all competition IDs
offset = 0
all_ids = []

with tqdm(total=total_competitions, desc="Fetching competition IDs", unit="competition") as pbar:
    while offset < total_competitions:
        query = f"""
        {{
            getCalendarEvents(
                startDate: "1983-01-01",
                endDate: "{today_date}",
                regionType: "world",
                hideCompetitionsWithNoResults: true,
                showOptionsWithNoHits: true,
                limit: {LIMIT},
                offset: {offset}
            ) {{
                results {{
                    id
                }}
            }}
        }}
        """
        retries = 0
        while retries < RETRIES:
            try:
                response = requests.post(url, headers=headers, json={"query": query}, timeout=TIMEOUT)
                if response.status_code == 200:
                    data = response.json()
                    if "data" in data and "getCalendarEvents" in data["data"]:
                        results = data["data"]["getCalendarEvents"]["results"]
                        all_ids.extend(result['id'] for result in results)
                        offset += LIMIT
                        pbar.update(len(results))
                        break
                    else:
                        logging.error("Unexpected response structure: %s", data)
                        break
                else:
                    logging.error("Query failed with status code %s.", response.status_code)
                    break
            except requests.Timeout:
                retries += 1
                wait_time = 2 ** retries
                logging.warning("TimeoutError: Retrying in %s seconds...", wait_time)
                time.sleep(wait_time)

# Print all fetched IDs to verify the results
print(all_ids)
