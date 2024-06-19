import src.utils.mongo_utils as mongo_utils
import os
import finnhub

from dotenv import load_dotenv

from scraper_functions import load_path
load_path()

load_dotenv()

api_key = os.getenv('FINNHUB_API_KEY')
finnhub_client = finnhub.Client(api_key=api_key)

all_tickers = finnhub_client.stock_symbols(exchange='US')

client = mongo_utils.connect_mongo()

for company in all_tickers:
    mongo_utils.insert_data_into_collection(
        client=client,
        db_name='tickers',
        collection_name='all_tickers',
        d_id=company['symbol'],
        name=company['description'],
        type=company['type']
    )
