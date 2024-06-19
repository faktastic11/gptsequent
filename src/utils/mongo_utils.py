from typing import List, Dict
import certifi
import os
import secrets

from bson import ObjectId
from dotenv import load_dotenv
from src.utils.loggers import reg_logger
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


load_dotenv()

logger = reg_logger('mongo_utils')


def connect_mongo(conn_str: str = None) -> MongoClient:
    """
    Connects to the MongoDB database using the provided environment variables.

    Returns:
        MongoClient: The MongoDB client object.
    """
    if conn_str is None:
        client = MongoClient(
            os.getenv('MONGO_CONN_STR'),
            uuidRepresentation="standard",
            tlsCAFile=certifi.where()
        )
    else:
        client = MongoClient(conn_str, tlsCAFile=certifi.where())
    logger.info('Connected to MongoDB')
    return client


def get_object_id() -> ObjectId:
    """Returns object ID

    Returns:
        ObjectId: Mongo object ID
    """
    return ObjectId(secrets.token_hex(12))


def insert_data_into_collection(
    client: MongoClient,
    db_name: str,
    collection_name: str,
    **kwargs
) -> None:
    """
    Inserts data into the specified collection of the MongoDB database.

    Args:
        client (MongoClient): The MongoDB client object.
        db_name (str): The name of the database where the collection resides.
        collection_name (str): The name of the collection to insert data into.
        d_id (str): The ID for the document.
        **kwargs: Additional key-value pairs representing the data to be inserted.

    Returns:
        None
    """
    db = client[db_name]
    collection = db[collection_name]
    try:
        result = collection.insert_one(kwargs)
        logger.info(
            f'Inserted transcript with ec_id {result.inserted_id} into collection {collection_name}')
        return result.inserted_id
    except DuplicateKeyError:
        logger.warning('key already exists, moving on')


def id_exists(
    client: MongoClient,
    db_name: str,
    collection_name: str,
    d_id: str
) -> bool:
    """
    Checks if a document with the specified ID exists in the given collection.

    Args:
        client (MongoClient): The MongoDB client object.
        db_name (str): The name of the database where the collection resides.
        collection_name (str): The name of the collection to check.
        d_id (str): The ID of the document to check.

    Returns:
        bool: True if the document exists, False otherwise.
    """
    db = client[db_name]
    collection = db[collection_name]
    document = collection.find_one({"_id": d_id})
    return document is not None


def get_data_from_collection(
    client: MongoClient,
    db_name: str,
    collection_name: str,
    projection: dict = None,
    query: dict = {},
    limit: int = 0
) -> List[Dict[str, object]]:
    """
    Retrieves data from the specified collection of the MongoDB database.

    Args:
        client (MongoClient): The MongoDB client object.
        db_name (str): The name of the database where the collection resides.
        collection_name (str): The name of the collection to query.
        projection (dict): A dictionary specifying which fields to include or exclude in the query results. ex: {ticker: 1, name: 0}
        query (dict): A dictionary representing the filtering criteria for the query. Default is an empty dictionary.
        limit (int): An optional parameter that limits the number of documents returned. Default is 0 (all documents).

    Returns:
        List[Dict[str, object]]: A list of dictionaries representing the retrieved documents.
    """
    db = client[db_name]
    collection = db[collection_name]
    documents = collection.find(query, projection, limit=limit)
    return list(documents)
