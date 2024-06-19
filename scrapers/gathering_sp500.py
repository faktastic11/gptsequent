import src.utils.mongo_utils as mongo_utils
import os
import finnhub

from dotenv import load_dotenv

from scraper_functions import load_path
load_path()

load_dotenv()

api_key = os.getenv('FINNHUB_API_KEY')
finnhub_client = finnhub.Client(api_key=api_key)

sp500 = finnhub_client.indices_const(symbol="^GSPC")

client = mongo_utils.connect_mongo()

for company in sp500['constituentsBreakdown']:
    mongo_utils.insert_data_into_collection(
        client=client,
        db_name='tickers',
        collection_name='sp500',
        d_id=company['symbol'],
        name=company['name'],
        weight=company['weight']
    )
