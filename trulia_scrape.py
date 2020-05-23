import datetime
import logging
import logging.handlers
import numpy as np
import os
import pandas as pd
import requests
import time
import functools

from bs4 import BeautifulSoup
from time import sleep
from tqdm import tqdm

# set path to current working directory for cron job
os.chdir(os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s : %(name)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
file_handler = logging.handlers.RotatingFileHandler('logs/scraper.log', maxBytes=10485760, backupCount=12)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

headers = {'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
           'accept-encoding': 'gzip, deflate, sdch, br',
           'accept-language': 'en-GB,en;q=0.8,en-US;q=0.6,ml;q=0.4',
           'cache-control': 'max-age=0',
           'upgrade-insecure-requests': '1',
           'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.131 Safari/537.36'}

base_url = 'https://www.trulia.com'
page_url = '/for_rent/Austin,TX/'


def function_timer(func):
    """Prints runtime of the decorated function."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        value = func(*args, **kwargs)
        elapsed_time = time.time() - start_time
        if logger:
            logger.info(f"Elapsed time: {round(elapsed_time/60,2)} minutes for function: '{repr(func.__name__)}'")
        else:
            print(f"Elapsed time: {round(elapsed_time/60,2)} minutes for function: '{repr(func.__name__)}'")
        return value
    return wrapper


@function_timer
def get_url_list(base_url, page_url):
    '''Gets a list of urls from main page to scrape.'''
    url_list = []
    last_page = False
    while last_page is False:
        try:
            response = requests.get(base_url + page_url, headers=headers)
        except (ConnectionError, ConnectionResetError):
            logger.exception('Error getting URL:')
            continue

        if response.status_code != 200:
            logger.info(f'Failed: {response.status_code}')
        else:
            soup = BeautifulSoup(response.content, 'lxml')

        for div in soup.find_all('div', {'data-hero-element-id': 'srp-home-card', 'data-hero-element-id': 'false'}):
            url = div.find('a').attrs['href']
            url_list.append(url)

        # check if last page and exit while loop
        if soup.find('a', {'aria-label': 'Next Page'}):
            last_page = False
            page_url = soup.find('a', {'aria-label': 'Next Page'})['href']
            sleep(.1)
        else:
            last_page = True
    return url_list


def get_apartment_data(base_url, current_url):
    '''Gets apartment data for the url specified'''
    try:
        response = requests.get(base_url + current_url, headers=headers)
    except (ConnectionError, ConnectionResetError):
        logger.exception('Error getting apartment data:')
    if response.status_code != 200:
        logger.info(f'Failed: {response.status_code}')
    else:
        soup = BeautifulSoup(response.content, 'lxml')

    apartment_list = []

    for floor_plan_table in soup.find_all('table', {'data-testid': 'floor-plan-group'}):
        for tr in floor_plan_table.find_all('tr'):

            unit = tr.find('div', {'color': 'highlight'}).text

            sqft = tr.find('td', {'class': 'FloorPlanTable__FloorPlanFloorSpaceCell-sc-1ghu3y7-5'}).text

            bed = tr.find_all('td', {'class': 'FloorPlanTable__FloorPlanFeaturesCell-sc-1ghu3y7-4'})[0].text

            bath = tr.find_all('td', {'class': 'FloorPlanTable__FloorPlanFeaturesCell-sc-1ghu3y7-4'})[1].text

            price = tr.find_all('td', {'class': 'FloorPlanTable__FloorPlanCell-sc-1ghu3y7-2',
                                       'class': 'FloorPlanTable__FloorPlanSMCell-sc-1ghu3y7-8'},
                                limit=2)[1].text

            name = soup.find('span', {'data-testid': 'home-details-summary-headline'}).text

            address = soup.find_all('span', {'data-testid': 'home-details-summary-city-state'})[0].text

            city_state_zip = soup.find_all('span', {'data-testid': 'home-details-summary-city-state'})[1].text

            city, state, zipcode = city_state_zip.replace(',', '').rsplit(maxsplit=2)

            description = soup.find('div', {'data-testid': 'home-description-text-description-text'}).text

            details = [detail.text for detail in soup.find_all(
                'li', {'class': 'FeatureList__FeatureListItem-iipbki-0 dArMue'}
            )]
            details = ' ,'.join(details)

            apartment_url = base_url + current_url
            date = str(datetime.datetime.now().date())
            apartment_list.append([name, address, unit, sqft, bed, bath, price, city,
                                   state, zipcode, description, details, apartment_url, date])
    return apartment_list


@function_timer
def get_all_apartments(url_list):
    '''Wrapper function using "get_apartment_data" function to get data for all apartments in "url_list"'''
    apts_data = []
    for i, current_url in enumerate(tqdm(url_list), start=1):
        if i % 500 == 0:
            logger.info(f'URL {i} of {len(url_list)}')
        sleep(.05)
        try:
            apts_data.extend(get_apartment_data(base_url, current_url))
        except Exception:
            logger.exception('Error adding data to list:')
            continue
    return apts_data


def create_df(data):
    df = pd.DataFrame(data,
                      columns=['name', 'address', 'unit', 'sqft', 'bed', 'bath', 'price',
                               'city', 'state', 'zipcode', 'description', 'details', 'url', 'date'])
    return df


def df_formatter(df):
    '''Formats the dataframe to remove special characters, spaces, and NaN values.
       Removes units with price ranges, rather than one specified price.'''

    df.sqft = df.sqft.str.replace('sqft', '').str.replace(',', '').str.strip()
    mask = df.sqft.str.contains('-')
    df.loc[mask, 'sqft'] = df[mask]['sqft'].apply(lambda x: np.mean(list(map(int, x.split('-')))))
    df.price = df.price.str.replace('Contact', '')
    df.price = df.price.str.replace('$', '').str.replace(',', '').str.strip()
    df.bath = df.bath.str.replace('ba', '').str.strip()
    df.bed = df.bed.str.replace('bd', '').str.lower().replace('studio', 0).str.strip()
    df.bed = df.bed.replace(np.nan, 0)
    df = df[~df.price.str.contains('-', na=False)]
    df = df.replace(' ', '')  # whitespace to blank
    df = df.replace('', np.nan)  # blank to NaN
    df = df.dropna()  # drop NaN rows

    return df


def df_converter(df):
    '''Converts rows to numeric and float for calculations'''
    df = df.astype({'sqft': 'int32', 'price': 'int32', 'bath': 'float32', 'bed': 'float32'})

    return df


def save_to_csv(df):
    '''Saves cleaned dataframe to csv file in "daily_scrape_files" folder"'''
    scraped_date = str(datetime.datetime.now().date())
    scraped_page = page_url.replace('/', '_').replace(',', '')
    df.to_csv(f'daily_scrape_files/apartments{scraped_page}{scraped_date}.csv', index=False)


@function_timer
def main():
    logger.info('PROGRAM STARTED')

    logger.info('Getting URL list')
    url_list = get_url_list(base_url, page_url)
    logger.info(f'URLs retrieved: {len(url_list)}')

    logger.info('Getting apartment data from url_list')
    apts_data = get_all_apartments(url_list)
    logger.info(f'Apartments retrieved: {len(apts_data)}')

    logger.info('Creating DataFrame from apartment list')
    df = create_df(apts_data)

    logger.info('Formatting Dataframe')
    df_fmt = df_formatter(df)

    logger.info('Converting Dataframe')
    df_cvt = df_converter(df_fmt)

    logger.info('Saving scraped data to csv')
    save_to_csv(df_cvt)

    logger.info('PROGRAM FINISHED')


if __name__ == "__main__":
    main()
