import src.utils.mongo_utils as mongo_utils
import logging
import re
import os
import sys
import time
import requests

from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from pymongo import MongoClient
from selenium import webdriver

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.webdriver import WebDriver


def create_driver() -> WebDriver:
    # Configure Selenium webdriver options
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # Run Chrome in headless mode

    # Set the path to your chromedriver executable
    # Configure Selenium webdriver
    # Replace with the path to your chromedriver executable
    driver = webdriver.Chrome(options=chrome_options)
    logging.info('driver created')
    return driver


def find_quarter(text: str) -> str:
    quarter = re.search(r'[qQ][0-4] [0-9]+', text)
    quarter = quarter.group()
    return quarter


def load_path() -> None:

    # Get the absolute path of the parent directory
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    # Add the parent directory to the system path
    sys.path.append(parent_dir)


load_path()


def scrape_ticker_transcript(driver: WebDriver, client: MongoClient, ticker: str, num_transcripts: int) -> dict:
    logging.info(f'Working on {ticker}')
    quarter = ''
    all_transcripts = {}

    # find which exchange this ticker is in
    exchanges = ['nasdaq', 'nyse', 'amex']
    for exchange in exchanges:
        search_url = f'https://www.fool.com/quote/{exchange}/{ticker}/#quote-earnings-transcripts'
        driver.get(search_url)

        if driver.title != '404':
            break

    if driver.title == '404':
        logging.warning(f'Could not find exchange for {ticker}')
        logging.debug(driver.title)
        return {}

    # Extract the link to the latest earnings call transcript
    try:
        earnings_section = driver.find_element(
            By.ID, 'quote-earnings-transcripts')

        more_earnings_button = earnings_section.find_element(
            By.CLASS_NAME, 'load-more-button')

        earnings_container = earnings_section.find_element(
            By.ID, 'earnings-transcript-container')

        earnings_data_page = earnings_container.find_element(
            By.CLASS_NAME, 'page')

        transcripts_obtained = earnings_data_page.find_elements(
            By.TAG_NAME, 'a')
        logging.info("finding transcripts")
    except:
        logging.warning(f'Could not find earnings transcript for {ticker}')
        return {}

    # make sure that there arre enough transcript links available
    while len(transcripts_obtained) < num_transcripts:
        if not more_earnings_button.is_displayed():
            logging.warning('The "load more" button is not visible.')
            break

        more_earnings_button.click()
        transcripts_obtained = earnings_data_page.find_elements(
            By.TAG_NAME, 'a')

        if len(transcripts_obtained) >= num_transcripts:
            logging.info('found all transcripts required')
            break

    for transcript_link in transcripts_obtained:

        title_div = transcript_link.find_element(By.TAG_NAME, 'div')
        quarter = find_quarter(str(title_div.text))
        quarter = quarter.replace(' ', '_')

        # check if the earnings call is already in db
        exists = mongo_utils.id_exists(
            client=client,
            db_name='transcripts',
            collection_name=f'transcripts_{ticker}',
            d_id=f'{ticker}_{quarter}'
        )

        if exists:
            logging.info('transcript already in DB')
            continue

        logging.info(f'parsing {quarter} transcript')
        # Wait for the page to load after clicking
        transcript_url = transcript_link.get_attribute('href')
        logging.debug(transcript_url)
        transcript_site = requests.get(transcript_url)
        time.sleep(2)

        # Find the transcript content within the specific div and p tags
        transcript_soup = BeautifulSoup(transcript_site.text, 'html.parser')
        transcript_div = transcript_soup.find(
            'div', class_='tailwind-article-body')

        paragraphs = []
        first_break_found = False

        datetime_recorded = ''

        for tag in transcript_div.find_all(['p', 'br']):
            if tag.name == 'br' and not first_break_found:
                first_break_found = True
            elif tag.name == 'p' and first_break_found:
                paragraphs.append(tag.get_text())
            elif tag.name == 'p' and not first_break_found and not datetime_recorded:
                date_recorded_tag = tag.find('span', {"id": "date"})
                time_recorded_tag = tag.find('em', {"id": "time"})
                if date_recorded_tag and time_recorded_tag:
                    date_recorded = date_recorded_tag.get_text(strip=True)
                    time_recorded = time_recorded_tag.get_text(strip=True)
                    datetime_recorded = date_recorded + ' ' + time_recorded

        logging.info(f'{quarter} transcript parsed')
        all_transcripts[quarter] = {
            'transcript': paragraphs, 'time': datetime_recorded}

    return all_transcripts
