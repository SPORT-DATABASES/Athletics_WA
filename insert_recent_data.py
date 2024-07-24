
## This script is for updating the WA database daily with new information
## from the most recent date in the MySQL database to todays date
## This code is run in Githhub actions on a schedule

import aiohttp
import asyncio
import polars as pl
import nest_asyncio
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm
import random
import time
import os
import aiofiles
import logging
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, MetaData, Table
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from webdriver_manager.microsoft import EdgeChromiumDriverManager
import time
import json

#### PART 0.5 - GET THE API AND REQUEST URL USING SELENIUM #####

# Configure Selenium WebDriver for Edge
options = EdgeOptions()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')

# Set logging preferences directly in the options
capabilities = DesiredCapabilities.EDGE.copy()
capabilities['ms:loggingPrefs'] = {'performance': 'ALL'}
options.set_capability("ms:loggingPrefs", {'performance': 'ALL'})

# Initialize WebDriver using the most recent WebDriver for Edge
driver = webdriver.Edge(service=EdgeService(EdgeChromiumDriverManager().install()), options=options)

# Open the webpage
url = 'https://worldathletics.org/competition/calendar-results'
driver.get(url)

# Wait for the page to load
time.sleep(10)  # Adjust the sleep time as necessary

# Extract network logs
logs = driver.get_log('performance')

# Filter out the necessary logs
for log in logs:
    log_json = json.loads(log['message'])['message']
    if log_json['method'] == 'Network.requestWillBeSent':
        request = log_json['params']['request']
        if request['method'] == 'POST' and 'graphql' in request['url']:
            headers = request['headers']
            request_url = request['url']
            api_key = headers.get('x-api-key')
            if api_key:
                print(f"Request URL: {request_url}")
                print(f"x-api-key: {api_key}")
                break


## PART 1. QUERY THE MYSQL DATABASE TO GET THE MOST RECENT DATE OF INFORMATION

# Load environment variables
load_dotenv()

# Database connection details
host = 'sportsdb-sports-database-for-web-scrapes.g.aivencloud.com'
port = 16439
user = 'avnadmin'
password = os.getenv('DB_PASSWORD')
database = 'defaultdb'
ca_cert_path = 'ca.pem'

# Create SQLAlchemy engine
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', connect_args={'ssl': {'ca': ca_cert_path}})

# Query the database to find the most recent date in the historical data
query = text('''
    SELECT MAX(Start_Date) as max_date 
    FROM WA_competition_results
''')
with engine.connect() as connection:
    result = connection.execute(query)
    most_recent_date = result.fetchone()[0]

# Print the most recent date
print(f"The most recent date is: {most_recent_date}")

# Assuming most_recent_date is already defined
most_recent_date = datetime.strptime('2024-07-24', '%Y-%m-%d')  # Example date

# Calculate the start date, 14 days before the most recent date
start_date = (most_recent_date - timedelta(days=14)).strftime('%Y-%m-%d')

# Set the end date for scraping
end_date = datetime.now().strftime('%Y-%m-%d')

nest_asyncio.apply()

# Setup logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# Endpoint URL and headers
url = request_url

headers = {
    "Content-Type": "application/json",
    "x-api-key": f"{api_key}"
}

# Retry settings
RETRIES = 5
TIMEOUT = 10  # seconds
CONCURRENT_REQUESTS = 100
PAUSE_FREQUENCY = 1000  # pause after every 500 requests
PAUSE_TIME_MIN = 5  # minimum pause time in seconds
PAUSE_TIME_MAX = 10  # maximum pause time in seconds
CHUNK_SIZE = 5000  # number of results to write to disk at once

# Fetch the total number of competitions
async def get_total_competitions(session):
    today_date = datetime.today().strftime('%Y-%m-%d')

    query = f"""
    {{
            getCalendarEvents(
            startDate: "{start_date}",
            endDate: "{end_date}",
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
    
    async with session.post(url, json={'query': query}, headers=headers, timeout=TIMEOUT) as response:
        if response.status == 200:
            data = await response.json()
            if "data" in data and "getCalendarEvents" in data["data"]:
                return data["data"]["getCalendarEvents"]["hits"]
            else:
                logging.error("Unexpected response structure: %s", data)
        else:
            logging.error("Query failed with status code %s.", response.status)
    return 0

# Fetch all competition IDs
async def fetch_competition_ids(session, total_competitions, limit=200):
    offset = 0
    all_ids = []

    with tqdm(total=total_competitions, desc="Fetching competition IDs", unit="competition") as pbar:
        while offset < total_competitions:
            query = f"""
            {{
                getCalendarEvents(
                    startDate: "{start_date}",
                    endDate: "{end_date}",
                    regionType: "world",
                    hideCompetitionsWithNoResults: true,
                    showOptionsWithNoHits: true,
                    limit: {limit},
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
                    async with session.post(url, json={'query': query}, headers=headers, timeout=TIMEOUT) as response:
                        if response.status == 200:
                            data = await response.json()
                            if "data" in data and "getCalendarEvents" in data["data"]:
                                results = data["data"]["getCalendarEvents"]["results"]
                                all_ids.extend(result['id'] for result in results)
                                offset += limit
                                pbar.update(len(results))
                                break
                            else:
                                logging.error("Unexpected response structure: %s", data)
                                break
                        else:
                            logging.error("Query failed with status code %s.", response.status)
                            break
                except asyncio.TimeoutError:
                    retries += 1
                    wait_time = 2 ** retries
                    logging.warning("TimeoutError: Retrying in %s seconds...", wait_time)
                    await asyncio.sleep(wait_time)

    return all_ids

# Fetch results for a single competition
async def fetch_competition_results(session, competition_id, semaphore, day="null", event_id="null"):
    query = f"""
    {{
        getCalendarCompetitionResults(competitionId: {competition_id}, day: {day}, eventId: {event_id}) {{
            competition {{
                name
                venue
                startDate
                endDate
            }}
            eventTitles {{
                eventTitle
                events {{
                    event
                    races {{
                        race
                        results {{
                            competitor {{
                                name
                                iaafId
                            }}
                            mark
                            wind
                            nationality
                            place
                            points
                            qualified
                            records
                            remark
                        }}
                    }}
                }}
            }}
        }}
    }}
    """
    retries = 0
    while retries < RETRIES:
        try:
            async with semaphore:
                async with session.post(url, json={'query': query}, headers=headers, timeout=TIMEOUT) as response:
                    if response.status == 200:
                        response_json = await response.json()
                        if 'errors' in response_json:
                            logging.warning("-")
                        if response_json.get('data', {}).get('getCalendarCompetitionResults') is None:
                            logging.info("No competition results found for competition ID %s", competition_id)
                            return None
                        return response_json
                    else:
                        logging.error("Failed to fetch results for competition %s with status code %s", competition_id, response.status)
                        break
        except asyncio.TimeoutError:
            retries += 1
            wait_time = 2 ** retries
            logging.warning("TimeoutError: Retrying in %s seconds...", wait_time)
            await asyncio.sleep(wait_time)
    return None

# Write data to parquet file
async def write_to_parquet(all_results):
    temp_file = 'competition_results_temp.parquet'
    if os.path.exists(temp_file):
        existing_df = pl.read_parquet(temp_file)
        new_df = pl.DataFrame(all_results)
        combined_df = pl.concat([existing_df, new_df])
        combined_df.write_parquet(temp_file)
    else:
        new_df = pl.DataFrame(all_results)
        new_df.write_parquet(temp_file)

# Main asynchronous function to fetch all data
async def main():
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=CONCURRENT_REQUESTS)) as session:
        # Get total number of competitions
        total_competitions = await get_total_competitions(session)
        logging.info("Total competitions: %d", total_competitions)

        # Fetch all competition IDs
        competition_ids = await fetch_competition_ids(session, total_competitions)
        logging.info("Fetched %d competition IDs", len(competition_ids))

        # Fetch results for all competitions with a progress bar
        all_results = []

        async def fetch_and_process_competition(competition_id, semaphore):
            competition_response = await fetch_competition_results(session, competition_id, semaphore)
            if competition_response is not None and 'data' in competition_response:
                comp_results = competition_response['data'].get('getCalendarCompetitionResults')
                if comp_results:
                    competition_info = {
                        "Competition ID": competition_id,
                        "Competition Name": str(comp_results['competition']['name']),
                        "Venue": str(comp_results['competition']['venue']),
                        "Start Date": str(comp_results['competition']['startDate']),
                        "End Date": str(comp_results['competition']['endDate'])
                    }
                    for event_title in comp_results['eventTitles']:
                        for ev in event_title['events']:
                            for race in ev['races']:
                                for result in race['results']:
                                    result_info = {
                                        "Event Title": str(event_title['eventTitle']),
                                        "Event": str(ev['event']),
                                        "Race": str(race['race']),
                                        "Competitor Name": str(result['competitor']['name']),
                                        "IAAF ID": str(result['competitor'].get('iaafId', '')),
                                        "Mark": str(result['mark']),
                                        "Wind": str(result['wind']),
                                        "Nationality": str(result['nationality']),
                                        "Place": str(result['place']),
                                        "Points": str(result['points']),
                                        "Qualified": str(result['qualified']),
                                        "Records": str(result['records']),
                                        "Remark": str(result['remark'])
                                    }
                                    all_results.append({**competition_info, **result_info})
            # Write data to disk periodically to reduce memory usage
            if len(all_results) >= CHUNK_SIZE:
                await write_to_parquet(all_results)
                all_results.clear()

        # Use tqdm to show progress
        semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
        tasks = [fetch_and_process_competition(competition_id, semaphore) for competition_id in competition_ids]
        for idx, f in enumerate(tqdm_asyncio.as_completed(tasks, desc="Fetching competition results", unit="competition")):
            await f
            if (idx + 1) % PAUSE_FREQUENCY == 0:
                pause_time = random.uniform(PAUSE_TIME_MIN, PAUSE_TIME_MAX)
                logging.info("Pausing for %.2f seconds...", pause_time)
                await asyncio.sleep(pause_time)

        # Write remaining data to disk
        if all_results:
            await write_to_parquet(all_results)

        logging.info("Fetched results for all competitions.")

        # Combine all temporary files into the final parquet file
        df = pl.read_parquet('competition_results_temp.parquet')
        new_column_names = {col: col.replace(" ", "_") for col in df.columns}
        df = df.rename(new_column_names)
        df.write_parquet('WA_update_results.parquet')
        os.remove('competition_results_temp.parquet')

        logging.info("Saved results to 'WA_updates.parquet'.")

# Run the main function
asyncio.run(main())

## PART 2. WRITE TO DATABASE ####################


# Function to delete existing records from the start date

def delete_existing_records(start_date, engine):
    delete_query = text('''
        DELETE FROM WA_competition_results
        WHERE Start_Date >= :start_date
    ''')
    try:
        with engine.connect() as connection:
            with connection.begin() as transaction:  # Ensure the transaction is explicitly handled
                result = connection.execute(delete_query, {"start_date": start_date})
                deleted_rows = result.rowcount
                print(f"Deleted {deleted_rows} records from {start_date} onward.")
                return deleted_rows
    except SQLAlchemyError as e:
        print(f"Error occurred while deleting records: {e}")
        return 0


# Delete records from the start date and get the number of rows deleted
deleted_rows = delete_existing_records(start_date, engine)
print(f"Number of rows deleted: {deleted_rows}")
# Check new data
df_update = pl.read_parquet('WA_update_results.parquet')

# Load the new data from the Parquet file
df = pl.read_parquet('WA_update_results.parquet')
df = df.unique()

# Convert Start_Date and End_Date to date format
df = df.with_columns([
    pl.col("Start_Date").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("Start_Date"),
    pl.col("End_Date").cast(pl.Utf8).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("End_Date")
])

df = df.sort(by=['Start_Date', 'Competition_ID', 'Event'], descending=[True, False, False])

df = df.with_columns(
    pl.col("Place").str.replace(r'\.$', '').str.strip_chars().alias("Place"))

# Rename Event to Event_Description
df = df.rename({'Event': 'Description_Event'})
df = df.rename({'Event_Title': 'Title_Event'})

# Split the Event_Description into Gender and Event
df = df.with_columns([
    pl.col('Description_Event').str.split_exact(" ", 1).struct.field("field_0").alias("Gender"),
    pl.col('Description_Event').str.split_exact(" ", 1).struct.field("field_1").alias("Event")
])

# Replace "Men's" with "Male" and "Women's" with "Female" in Gender column
df = df.with_columns(
    pl.col('Gender')
    .str.replace("Men's", "Male")
    .str.replace("Women's", "Female"))

pandas_df = df.to_pandas()

# Function to insert data in batches
def insert_in_batches(df, table_name, engine, batch_size):
    total_rows = len(df)
    for start in range(0, total_rows, batch_size):
        end = start + batch_size
        batch = df[start:end]
        try:
            batch.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print(f"Inserted rows {start + 1} to {min(end, total_rows)}")
        except SQLAlchemyError as e:
            print(f"Error occurred while inserting rows {start + 1} to {min(end, total_rows)}: {e}")

# Example usage
try:
    batch_size = 1000  # Define the batch size
    insert_in_batches(pandas_df, 'WA_competition_results', engine, batch_size)
    print("Updated Data inserted successfully into the MySQL database.")
except SQLAlchemyError as e:
    print(f"Error occurred while inserting data: {e}")

os.remove('WA_update_results.parquet')

#######################################################################

query = text('''
    SELECT MAX(Start_Date) as max_date 
    FROM WA_competition_results
''')
with engine.connect() as connection:
    result = connection.execute(query)
    most_recent_date = result.fetchone()[0]
    
print(most_recent_date)

polars_df = pl.from_pandas(pandas_df)


