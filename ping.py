import requests
from datetime import datetime

# Endpoint URL and headers
url = "https://graphql-prod-4609.prod.aws.worldathletics.org/graphql"
headers = {
    "Content-Type": "application/json",
    "x-api-key": "da2-4enkl5vjkbczbfowagsi5l4qpy"
}

# Get today's date in the required format
today_date = datetime.today().strftime('%Y-%m-%d')

# Sample query
query = f"""
{{
    getCalendarEvents(
        startDate: "2023-01-01",
        endDate: "{today_date}",
        regionType: "world",
        hideCompetitionsWithNoResults: true,
        showOptionsWithNoHits: true,
        limit: 1,
        offset: 0
    ) {{
        hits
    }}
}}
"""

# Sending the POST request
response = requests.post(url, headers=headers, json={"query": query})

# Check if the request was successful
if response.status_code == 200:
    # Print the JSON response
    print(response.json())
else:
    print(f"Request failed with status code {response.status_code}")
    print(response.text)
