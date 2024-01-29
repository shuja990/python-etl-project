from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime

# Code for ETL operations on Country-GDP data
# Importing the required libraries

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
exchange_csv = './exchange_rate.csv'
output_path = './Largest_banks_data.csv'
log_path = './code_log.txt'

sql_connection = sqlite3.connect(db_name)

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    print(timestamp + ' : ' + message + '\n')
    with open(log_path,"a") as f: 
        f.write(timestamp + ' : ' + message + '\n')
    
def extract(url, table_attribs):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')

    # Find the heading 'By market capitalization'
    heading = soup.find(lambda tag: tag.name == "h2" and "By market capitalization" in tag.text)
    if not heading:
        raise ValueError("Heading 'By market capitalization' not found")

    # Find the next table after this heading
    table = heading.find_next('table')
    if not table:
        raise ValueError("Table not found after heading")

    # Initialize DataFrame
    df = pd.DataFrame(columns=table_attribs)

    # Iterate through each row in the table body
    for row in table.find_all('tr')[1:]:  # Skip the header row
        cols = row.find_all('td')
        if cols:
            bank_name = cols[1].text.strip()
            market_cap = cols[2].text.strip()
            # Remove the last character (newline) and convert to float
            if market_cap:
                try:
                    market_cap = float(market_cap[:-1])
                except ValueError:
                    raise ValueError(f"Invalid market cap format: {market_cap}")

            data_dict = {"Name": bank_name,"MC_USD_Billion": market_cap}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)

    return df



def transform(df, csv_file_path):
    # Check if 'MC_USD_Billion' column exists in the DataFrame
    print(df.columns)
    if 'MC_USD_Billion' not in df.columns:
        raise KeyError("'MC_USD_Billion' column not found in DataFrame")

    # Read CSV data into a DataFrame
    exchange_rates_df = pd.read_csv(csv_file_path)

    # Convert the DataFrame to a dictionary
    exchange_rates = exchange_rates_df.set_index('Currency')['Rate'].to_dict()

    # Add new columns for GBP, EUR, and INR directly to the original DataFrame
    df['MC_GBP_Billion'] = [np.round(x * exchange_rates.get('GBP', 1), 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rates.get('EUR', 1), 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rates.get('INR', 1), 2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, output_path):
    try:
        df.to_csv(output_path, index=False)
        print(f"DataFrame successfully saved to {output_path}")
    except Exception as e:
        print(f"Error occurred while saving DataFrame to CSV: {e}")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    try:
        # Load the DataFrame into the specified table
        df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

    except Exception as e:
        print(f"Error occurred while loading data to SQLite: {e}")

def run_query(query_statement, sql_connection):
    # This function runs the query on the database table and prints the output
    try:
        cursor = sql_connection.cursor()
        cursor.execute(query_statement)
        rows = cursor.fetchall()
        for row in rows:
            print(f"{row} \n")
    except Exception as e:
        print(f"Error occurred while running query: {e}")
    finally:
        cursor.close()

log_progress("ETL process started")

log_progress("Data extraction started")
extracted_data = extract(url,table_attribs)
print("Extracted data \n", extracted_data)
log_progress("Data extraction completed")

log_progress("Data transformation started")
transformed_data = transform(extracted_data,exchange_csv)
print("Transformed data \n", transformed_data)
log_progress("Data transformation completed")
print(transformed_data['MC_EUR_Billion'][4])

log_progress("Data loading to CSV started")
load_to_csv(transformed_data,output_path)
log_progress("Data loading to CSV completed")


log_progress("Data loading to DB started")
load_to_db(transformed_data,sql_connection,table_name)
log_progress("Data loading to DB completed")

# Run SQL queries
queries = [
    "SELECT * FROM Largest_banks",
    "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
    "SELECT Name from Largest_banks LIMIT 5"
]

for query in queries:
    log_progress(f"Running query: {query} \n")
    run_query(query, sql_connection)

sql_connection.close()
