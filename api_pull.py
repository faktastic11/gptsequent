from datetime import datetime
import time
from bson import ObjectId
from pymongo import MongoClient
import requests

from src.utils.mongo_utils import connect_mongo, insert_data_into_collection


def fetch_transcript(ticker, quarter, year):
    # Construct the URL based on the function parameters
    url = f"https://discountingcashflows.com/api/transcript/{ticker}/{quarter}/{year}/"

    # Make the HTTP GET request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Extract the 'content' field from the JSON response
        # Parse the JSON response
        data = response.json()

        # Check if 'content' is in the response and split it into lines
        if 'content' in data:
            data['content'] = data['content'].split('\n')

        return data
    else:
        # Handle errors (e.g., 404 or other HTTP errors)
        return f"Failed to fetch data: {response.status_code}"


def check_transcripts(client: MongoClient, data: dict, year, quarter) -> bool:
    db = client['transcripts']
    collection = db['rawTranscripts']
    query = {
        "companyTicker": data["symbol"],
        "fiscalYear": year,
        "fiscalQuarter": quarter
    }
    exists = collection.find_one(query)
    if exists and len(exists) > 0:
        return True
    return False


def process_and_store_data(ticker, quarter, year, client):
    """
    Processes the API data and stores it in the MongoDB collection.
    """
    # Fetch data from API
    data = fetch_transcript(ticker, quarter, year)
    print(len(data))
    if len(data) == 0:
        print('failed to find transcript')
        return None
    data = data[0]

    # If error in fetching, return or log error
    if "error" in data:
        return data['error']

    # Prepare data for MongoDB insertion
    mongo_data = {
        "companyName": data["symbol"],
        "companyTicker": ticker,
        "dateOfRecord": data["date"],
        "fiscalYear": year,
        "fiscalQuarter": quarter,
        # Assuming 'content' is the transcript
        "transcript": data["content"].split('\n'),
        "createdAt": datetime.now(),
        "updatedAt": datetime.now(),
        "__v": 1 if check_transcripts(client, data, year, quarter) else 0,
        "_id": ObjectId()  # Generates a new object ID
    }

    # Insert data into MongoDB
    return insert_data_into_collection(client, "transcripts", "rawTranscripts", **mongo_data)


# Define your list of tickers
tickers = ["AAPL", "GOOG", "MSFT", "AMZN", "FB", "TSLA", "V", "JPM", "JNJ", "WMT", "PG",
           "UNH", "DIS", "NVDA", "HD", "PYPL", "BAC", "VZ", "INTC", "KO", "PFE", "NFLX", "CSCO", "XOM"]
# Define quarters
quarters = ["Q1", "Q2", "Q3", "Q4"]
# Define the range of years
years = [2021, 2022, 2023]

# Initialize MongoDB connection
mongo_client = connect_mongo()


def process_all_data() -> None:
    for year in years:
        for quarter in quarters:
            for ticker in tickers:
                print(f'processing {ticker}, {quarter}, {year}')
                result = process_and_store_data(
                    ticker, quarter, year, mongo_client)
                print(
                    f"Processed: {ticker}, {quarter}, {year}, Result: {result}")


process_all_data()
