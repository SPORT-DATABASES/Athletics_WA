import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database connection details
host = 'sportsdb-sports-database-for-web-scrapes.g.aivencloud.com'
port = 16439
user = 'avnadmin'
password = os.getenv('DB_PASSWORD')
database = 'defaultdb'
ca_cert_path = 'ca.pem'

# Load the Parquet file data
file_path = 'competition_data_historical.parquet'
df = pd.read_parquet(file_path)

df.columns = df.columns.str.replace(' ', '_')

# Define the column types
column_types = {
    "Competition_ID": 'int64',
    "Name": 'str',
    "Venue": 'str',
    "Date_Range": 'str',
    "Event_Title": 'str',
    "Event": 'str',
    "Race": 'str',
    "Competitor_Name": 'str',
    "IAAF_ID": 'float64',
    "Mark": 'str',
    "Wind": 'str',
    "Nationality": 'str',
    "Place": 'str',
    "Points": 'float64',
    "Qualified": 'bool',
    "Records": 'str',
    "Remark": 'str'
}

# Ensure the dataframe has the correct column types
df = df.astype(column_types)

# Create SQLAlchemy engine
engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}', connect_args={'ssl': {'ca': ca_cert_path}})

# Batch size for inserting data
batch_size = 10000

# Create table if it doesn't exist and truncate existing data
with engine.connect() as connection:
    connection.execute(text('''
    CREATE TABLE IF NOT EXISTS Athletics_WA_Data (
        `id` INT AUTO_INCREMENT PRIMARY KEY,
        `Competition_ID` BIGINT,
        `Name` VARCHAR(255),
        `Venue` VARCHAR(255),
        `Date_Range` VARCHAR(255),
        `Event_Title` VARCHAR(255),
        `Event` VARCHAR(255),
        `Race` VARCHAR(255),
        `Competitor_Name` VARCHAR(255),
        `IAAF_ID` DOUBLE,
        `Mark` VARCHAR(255),
        `Wind` VARCHAR(255),
        `Nationality` VARCHAR(255),
        `Place` VARCHAR(255),
        `Points` DOUBLE,
        `Qualified` BOOLEAN,
        `Records` VARCHAR(255),
        `Remark` VARCHAR(255),
        `last_updated` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )
    '''))

# Insert data in batches
for start in range(0, len(df), batch_size):
    end = start + batch_size
    batch_df = df[start:end]
    batch_df.to_sql('Athletics_WA_Data', con=engine, if_exists='append', index=False)
    print(f'Inserted rows {start} to {end}')

print('Data inserted successfully for Historical WA data.')
