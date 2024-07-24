
## This script is for loading in ALL information from World Atheltics
## from 1983 up to the present day.
## The data is stored in a parquet file named WA_historical_results.parquet
## Then the data is saved to the Aiven MYSQL database. The database is set up
## then data is sent to database

## Another file then inserts the most recent datas into the database.
## This file is only needed for initial start up, or re-initialization of the database.

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
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Date, MetaData, Table
from sqlalchemy.sql import text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

file_path = 'WA_historical_results.parquet'

try:
    if os.path.exists(file_path):
        os.remove(file_path)
        print(f"File {file_path} has been deleted.")
    else:
        print(f"File {file_path} does not exist.")
except Exception as e:
    print(f"An error occurred while trying to delete the file: {e}")

## PART 1. GET ALL THE COMPETITION IDS AND RESULTS USING ASYNCIO

nest_asyncio.apply()

# Setup logging
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')

# Endpoint URL and headers
url = "https://graphql-prod-4609.prod.aws.worldathletics.org/graphql"
headers = {
    "Content-Type": "application/json",
    "x-api-key": "da2-4enkl5vjkbczbfowagsi5l4qpy"
}

# Retry settings
RETRIES = 5
TIMEOUT = 10  # seconds
CONCURRENT_REQUESTS = 100
PAUSE_FREQUENCY = 1000  # pause after every 500 requests
PAUSE_TIME_MIN = 5  # minimum pause time in seconds
PAUSE_TIME_MAX = 10  # maximum pause time in seconds
CHUNK_SIZE = 5000  # number of results to write to disk at once

# Get today's date
today_date = datetime.today().strftime('%Y-%m-%d')

# Fetch the total number of competitions
async def get_total_competitions(session):
    today_date = datetime.today().strftime('%Y-%m-%d')

    query = f"""
    {{
            getCalendarEvents(
            startDate: "1983-01-01",
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
                    startDate: "1983-01-01",
                    endDate: "2024-07-19",
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
        df.write_parquet('WA_historical_results.parquet')
        os.remove('competition_results_temp.parquet')

        logging.info("Saved results to 'WA_historical_results.parquet'.")

# Run the main function
asyncio.run(main())

## PART 2. WRITE TO DATABASE ####################

# Load environment variables
load_dotenv()

# Database connection details
host = 'sportsdb-sports-database-for-web-scrapes.g.aivencloud.com'
port = 16439
user = 'avnadmin'
password = os.getenv('DB_PASSWORD')
database = 'defaultdb'
ca_cert_path = 'ca.pem'

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', connect_args={'ssl': {'ca': ca_cert_path}})

# Load the new data from the Parquet file
df = pl.read_parquet('WA_historical_results.parquet')
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

#########################################################

# Define and create the table, then truncate it to remove all existing data
try:
    with engine.connect() as connection:
        connection.execute(text('''
            CREATE TABLE IF NOT EXISTS WA_competition_results (
                `Row_id` INT AUTO_INCREMENT PRIMARY KEY,
                Competition_ID INT,
                Competition_Name VARCHAR(255),
                Venue VARCHAR(255),
                Start_Date DATE,
                End_Date DATE,
                Title_Event VARCHAR(255),
                Event VARCHAR(255),
                Race VARCHAR(255),
                Competitor_Name VARCHAR(255),
                IAAF_ID VARCHAR(255),
                Gender VARCHAR(255),
                Description_Event VARCHAR(255),
                Mark VARCHAR(255),
                Wind VARCHAR(255),
                Nationality VARCHAR(255),
                Place VARCHAR(255),
                Points VARCHAR(255),
                Qualified VARCHAR(255),
                Records VARCHAR(255),
                Remark VARCHAR(255),
                
                `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        '''))
        
        # Truncate the table to remove all existing data
        connection.execute(text('TRUNCATE TABLE WA_competition_results'))
        print("Table created and truncated successfully.")
except SQLAlchemyError as e:
    print(f"Error occurred while creating or truncating table: {e}")

# Function to insert data in batches with print statements
def insert_in_batches(df, table_name, engine, batch_size):
    total_rows = len(df)
    for start in range(0, total_rows, batch_size):
        end = start + batch_size
        batch = df[start:end]
        batch.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"Inserted rows {start + 1} to {min(end, total_rows)}")

# Insert the data into the MySQL database in batches
try:
    batch_size = 50000  # Define the batch size
    insert_in_batches(pandas_df, 'WA_competition_results', engine, batch_size)
    print("Data inserted successfully into the MySQL database.")
except SQLAlchemyError as e:
    print(f"Error occurred while inserting data: {e}")

os.remove('WA_historical_results.parquet')
