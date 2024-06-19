"""Mongo functions for getting transcripts from the database.
"""
from typing import List, Dict
from src.utils.mongo_utils import (
    connect_mongo,
    get_data_from_collection,
    get_object_id,
    insert_data_into_collection
)

from src.utils.loggers import reg_logger

logger = reg_logger('database_logger')


client = connect_mongo()


def get_all_tickers() -> List[str]:
    """Get all transcript tickers

    Args:
        client (MongoClient): Mongo client

    Returns:
        List[str]: List of tickets
    """
    # Get all companyTickers from the transcripts collection
    tickers = get_data_from_collection(
        client,
        db_name='transcripts',
        collection_name='rawTranscripts',
        projection={'companyTicker': 1, '_id': 0}
    )
    tickers=[doc['companyTicker'] for doc in tickers]
    logger.info(f'Got {len(tickers)} tickers')
    return tickers


def get_transcripts_by_ticker(
    ticker: str
) -> List[Dict]:
    """
    Gets all transcripts from the specified collection of the MongoDB database.

    Args:
        client (MongoClient): The MongoDB client object.
        ticker (str): Company ticker.

    Returns:
        List[Dict]: A list of all documents in the collection.
    """
    documents=get_data_from_collection(
        client,
        'transcripts',
        'rawTranscripts',
        projection={},
        query={'companyTicker': ticker}
    )
    logger.info(f'Got {len(documents)} transcripts for {ticker}')
    return documents


def insert_processed_transcripts(
    collection_name: str,
    data_dict: Dict
) -> None:
    """Insert processed transcripts into the database

    Args:
        client (MongoClient): Mongo client
        processed_transcripts (List[Dict]): List of processed transcripts
    """
    # Add _id to processed_transcripts dictionary
    insert_data_into_collection(
        client,
        db_name='transcripts',
        collection_name=collection_name,
        **data_dict
    )
    logger.debug(f'Inserted {data_dict} into processedTranscripts')


def get_line_items_by_ticker(
    ticker: str,
    fiscal_period: tuple = None
) -> List[Dict]:
    """Get line items by ticker

    Args:
        ticker (str): Company ticker

    Returns:
        List[Dict]: List of line items
    """
    if fiscal_period is not None:
        line_items = get_data_from_collection(
            client,
            db_name='transcripts',
            collection_name='processedTranscripts',
            projection={},
            query={'companyTicker': ticker, 'fiscal_year': fiscal_period[0], 'fiscal_quarter': fiscal_period[1]}
        )
        logger.info(f'Got {len(line_items)} line items for {ticker}')
        return line_items
    else:
        line_items = get_data_from_collection(
            client,
            db_name='transcripts',
            collection_name='processedTranscripts',
            projection={},
            query={'companyTicker': ticker}
        )
        logger.info(f'Got {len(line_items)} line items for {ticker}')
        return line_items
