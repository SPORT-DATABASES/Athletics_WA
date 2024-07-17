import os
import pandas as pd
import polars as pl
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime
import asyncio
import nest_asyncio
import worldathletics
import random
import time

nest_asyncio.apply()

client = worldathletics.WorldAthletics()

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
with engine.connect() as connection:
    result = connection.execute(text('''
        SELECT MAX(STR_TO_DATE(Date_Range, '%d %b %Y')) as max_date 
        FROM Athletics_WA_Data
    '''))
    most_recent_date = result.fetchone()[0]

# Set the end date for scraping
end_date = datetime.now().strftime('%Y-%m-%d')

# Assuming the dates are in '12 JUL 2024' format, parse the date
most_recent_date = datetime.strptime(most_recent_date, '%Y-%m-%d')
most_recent_date_str = most_recent_date.strftime('%d %b %Y')

async def scrape_data(start_date, end_date):
    limit = 17000
    batch_size = 100
    competition_data = []

    for offset in range(0, limit, batch_size):
        try:
            response = await client.get_calendar_events(
                hide_competitions_with_no_results=True,
                limit=batch_size,
                region_type='world',
                offset=offset,
                show_options_with_no_hits=False,
                order_direction='Ascending',
                competition_group_id=None,
                competition_subgroup_id=None,
                discipline_id=None,
                end_date=end_date,
                permit_level_id=None,
                query=None,
                ranking_category_id=None,
                region_id=None,
                start_date=start_date.strftime('%Y-%m-%d')
            )

            for key, event_data in response:
                if key == 'get_calendar_events' and event_data.results:
                    for event in event_data.results:
                        try:
                            competition_info = {
                                "Competition_ID": event.id,
                                "Name": event.name,
                                "Venue": event.venue,
                                "Date_Range": event.date_range
                            }

                            # Fetching competition results for each event
                            competition_response = await client.get_calendar_competition_results(competition_id=event.id)
                            
                            if competition_response.get_calendar_competition_results:
                                competition = competition_response.get_calendar_competition_results.competition
                                event_titles = competition_response.get_calendar_competition_results.event_titles
                                
                                for event_title in event_titles:
                                    for ev in event_title.events:
                                        for race in ev.races:
                                            for result in race.results:
                                                result_info = {
                                                    "Event_Title": event_title.event_title,
                                                    "Event": ev.event,
                                                    "Race": race.race,
                                                    "Competitor_Name": result.competitor.name,
                                                    "IAAF_ID": result.competitor.iaaf_id,
                                                    "Mark": result.mark,
                                                    "Wind": result.wind,
                                                    "Nationality": result.nationality,
                                                    "Place": result.place,
                                                    "Points": result.points,
                                                    "Qualified": result.qualified,
                                                    "Records": result.records,
                                                    "Remark": result.remark
                                                }
                                                competition_data.append({**competition_info, **result_info})
                        except Exception as e:
                            print(f"Error processing competition {event.id}: {e}")
                            continue

            print(f"Batch from offset {offset} to {offset + batch_size} completed.")

            # Introduce a random pause between batches
            pause_duration = random.uniform(5, 10)
            print(f"Pausing for {pause_duration:.2f} seconds to avoid rate limiting...")
            time.sleep(pause_duration)

        except Exception as e:
            print(f"Error fetching data: {e}")
            continue

    # Create a DataFrame from the competition data
    df = pd.DataFrame(competition_data)
    df.to_parquet('competition_data.parquet')
    print("All batches completed. Data has been saved to 'competition_data.parquet'.")    

# Scrape new data
asyncio.run(scrape_data(most_recent_date, end_date))

# Load the new data from the Parquet file
new_df = pl.read_parquet('competition_data.parquet')

# Rename columns to replace spaces with underscores
new_df.columns = new_df.columns.str.replace(' ', '_')

# Append new data to the historical data in the Aiven database
with engine.connect() as connection:
    new_df.to_pandas().to_sql('Athletics_WA_Data', con=engine, if_exists='append', index=False)

print('New data appended successfully to Athletics_WA_Data.')