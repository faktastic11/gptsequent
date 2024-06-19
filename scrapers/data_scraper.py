import src.utils.mongo_utils as mongo_utils
import logging
import os

from dotenv import load_dotenv

import scraper_functions
scraper_functions.load_path()

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s: %(message)s')

client = mongo_utils.connect_mongo()

driver = scraper_functions.create_driver()

stock_tickers = mongo_utils.get_data_from_collection(
    client=client,
    db_name='tickers',
    collection_name='sp500',
    projection={'ticker': 1},
    limit=100
)

earnings_calls = []
for company in stock_tickers:
    # Remove leading/trailing whitespace and newline characters
    ticker = company['_id'].strip().upper()
    transcripts = scraper_functions.scrape_ticker_transcript(
        driver=driver,
        client=client,
        ticker=ticker,
        num_transcripts=4)

    if transcripts:
        # Create collection if it doesn't exist
        collection_name = f'transcripts_{ticker}'

        # Insert transcripts into collection
        for period, transcript in transcripts.items():
            ec_id = f'{ticker}_{period}'
            mongo_utils.insert_data_into_collection(
                client=client,
                db_name=os.getenv('DB_NAME'),
                collection_name=collection_name,
                transcript=transcript['transcript'],
                time_recorded=transcript['time'],
                quarter=period,
                d_id=ec_id
            )

        earnings_calls.append(collection_name)

# Close the driver
driver.quit()

# Close the MongoDB connection
client.close()

# Print the list of saved table names
logging.info('Earnings call transcripts saved:')
for table_name in earnings_calls:
    logging.info(table_name)
