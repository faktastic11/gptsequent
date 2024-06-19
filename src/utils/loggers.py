"""General purpose utilites
"""
from dotenv import find_dotenv, load_dotenv
from datetime import datetime
from pymongo import MongoClient

import csv
import io
import os
import logging
import time


load_dotenv(dotenv_path=find_dotenv(), override=True)

BASE_DIR = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))))

# Ensure the logs folder exists
LOGS_DIR = os.path.join(BASE_DIR, 'logs')
if not os.path.exists(LOGS_DIR):
    os.makedirs(LOGS_DIR)


class OpenAICsvFormatter(logging.Formatter):
    def __init__(self):
        super().__init__()
        self.output = io.StringIO()
        self.writer = csv.writer(self.output)

    def format(self, record):
        ct = self.converter(record.created)
        s = time.strftime("%Y-%m-%d %H:%M:%S", ct)
        record.asctime = s
        logger_row = [os.getenv('ENVIRONMENT'),
                      record.asctime, record.levelname]
        split_message = list(record.msg.values())
        row = logger_row + split_message
        self.writer.writerow(row)
        data = self.output.getvalue()
        self.output.truncate(0)
        self.output.seek(0)
        return data.strip()


class MongoDBFormatter(logging.Formatter):
    def __init__(self, client: MongoClient, db_name: str, collection_name: str):
        super().__init__()
        self.client = client
        self.db_name = db_name
        self.collection_name = collection_name

    def format(self, record):
        ct = self.converter(record.created)
        s = time.strftime("%Y-%m-%d %H:%M:%S", ct)
        record.asctime = s

        meta = dict(record.msg)
        meta_data = {item.split(': ')[0].strip(): item.split(': ')[1].strip()
                     for item in meta['base_context'].split(';')}

        print('meta data:', meta_data)

        meta_data = {
            'created': meta['created'],
            'model': meta['model'],
            'Ticker': meta_data['Ticker'],
            'Transcript Fiscal Year': meta_data['Transcript Fiscal Year'],
            'Transcript Quarter': meta_data['Transcript Quarter'],
        }

        log_data = {
            'level': os.getenv('ENVIRONMENT'),
            'timestamp': record.asctime,
            'level': record.levelname,
            **{k: v for k, v in dict(record.msg).items() if k not in meta_data}
        }

        db = self.client[self.db_name]
        collection = db[self.collection_name]

        filter = {"Ticker": meta_data['Ticker'],
                  "Transcript Quarter": meta_data['Transcript Quarter'],
                  "Transcript Fiscal Year": meta_data['Transcript Fiscal Year']}
        update = {
            '$set': meta_data,
            '$push': {'logs': log_data}
        }

        collection.update_one(filter, update, upsert=True)
        return str(record.msg)


def openai_logger(logger_name: str, console: bool = True, client: MongoClient = None, db_name: str = None, collection_name: str = None):
    logger = logging.getLogger(logger_name)
    if logger.hasHandlers():
        logger.handlers.clear()

    # Console handler
    logger.setLevel(logging.DEBUG)
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_formatter = logging.Formatter(
            "%(name)s;%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S"
        )
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    # File handler
    environment = os.getenv('ENVIRONMENT')

    # File handler for DEV environment
    if environment == 'DEV':
        current_date = datetime.now().strftime("%Y%m%d")
        log_filename = os.path.join(
            LOGS_DIR, f"openai_{current_date}.csv")
        file_handler = logging.FileHandler(filename=log_filename, mode="a")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(OpenAICsvFormatter())
        logger.addHandler(file_handler)
    # MongoDB handler for PROD environment
    elif environment == 'PROD':
        if client is None or db_name is None or collection_name is None:
            raise ValueError(
                "MongoClient, db_name, collection_name, company_ticker, and quarter are required for PROD environment")
        mongo_handler = logging.StreamHandler()
        mongo_handler.setLevel(logging.DEBUG)
        mongo_formatter = MongoDBFormatter(
            client, db_name, collection_name)
        mongo_handler.setFormatter(mongo_formatter)
        logger.addHandler(mongo_handler)

    logger.propagate = False
    return logger


def reg_logger(logger_name: str):
    logger = logging.getLogger(logger_name)
    if logger.hasHandlers():
        logger.handlers.clear()

    # Console handler
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_formatter = logging.Formatter(
        "%(name)s;%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    current_date = datetime.now().strftime("%Y%m%d")
    log_filename = os.path.join(
        LOGS_DIR, f"logfile_{current_date}.txt")
    file_handler = logging.FileHandler(log_filename, mode="a")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(name)s;%(asctime)s;%(levelname)s;%(message)s", "%Y-%m-%d %H:%M:%S"
    )
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    logger.propagate = False
    return logger
