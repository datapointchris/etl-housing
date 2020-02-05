import pandas as pd
import numpy as np
import requests
import json
from time import sleep
from bs4 import BeautifulSoup
import sqlite3
import time
import datetime
import argparse
import logging
import os

# set path to current working directory for cron job
os.chdir(os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(name)s:%(message)s')
file_handler = logging.FileHandler('scraper.log')
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


def get_url_list(base_url, page_url):
    '''Gets a list of urls from main page to scrape.'''
    
    start_time = time.time()
    url_list = []
    last_page = False
    logger.info('Getting url_list')
    while (last_page == False):
        
        response = requests.get(base_url+page_url, headers=headers)
        
        if response.status_code != 200:
            logger.info("Failed: ", response.status_code)
        else:
            soup = BeautifulSoup(response.content, 'lxml')


        

        container = soup.find('div', {'data-testid': 'search-result-list-container'})

        for div in soup.find_all('div', {'data-hero-element-id': 'srp-home-card', 'data-hero-element-id':'false'}):

            url = div.find('a').attrs['href']
            url_list.append(url)

        # check if last page and exit while loop
            
        if soup.find('a', {'aria-label': 'Next Page'}):
            last_page = False
            page_url = soup.find('a', {'aria-label': 'Next Page'})['href']
            sleep(.5)
        else:

            last_page = True
            logger.info(f'Done getting url_list, length of {len(url_list)}')
            elapsed_time = time.time() - start_time
            logger.info(f'Elapsed time: {round(elapsed_time/60,2)} minutes')

    return url_list



def get_apartment_data(base_url, current_url):
    '''Gets apartment data for the url specified'''
    
    response = requests.get(base_url+current_url, headers=headers)
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
            
            city, state, zipcode = city_state_zip.replace(',','').rsplit(maxsplit=2)
            
            description = soup.find('div', {'data-testid': 'home-description-text-description-text'}).text

            details = [detail.text for detail in soup.find_all(
                                                        'li', {'class': 'FeatureList__FeatureListItem-iipbki-0 dArMue'}
                                                            )]
            details = ' ,'.join(details)

            apartment_url = base_url + current_url
            
            date = str(datetime.datetime.now().date())

            apartment_list.append([name, address, unit, sqft, bed, bath, price, city, state, zipcode, description, details, apartment_url, date])
    
    return apartment_list



def get_all_apartments(url_list):
    '''Wrapper function using "get_apartment_data" function to get data for all apartments in "url_list"'''
    apts_data = []
    start_time = time.time()
    logger.info(f'Getting apartment data from url_list')
    i=1
    for current_url in url_list:
        if i % 500 == 0:
            logger.info(f'URL {i} of {len(url_list)}')
        i += 1
        sleep(.05)
        try:
            apts_data.extend(get_apartment_data(base_url, current_url))
        except Exception:
            logger.exception('Error adding data to list')
            continue

    logger.info(f'Finished getting apartment data. Total apartments: {len(apts_data)}')
    elapsed_time = time.time() - start_time
    logger.info(f'Elapsed time: {round(elapsed_time/60,2)} minutes')
    
    return apts_data



def create_df(data):
    logger.info('Creating DataFrame from apartment list.')
    df = pd.DataFrame(data,
                     columns=['name', 'address', 'unit', 'sqft', 'bed', 'bath', 'price', 
                              'city', 'state', 'zipcode', 'description', 'details', 'url', 'date'])
    return df



def df_formatter(df):
    '''Formats the dataframe to remove special characters, spaces, and NaN values.
       Removes units with price ranges, rather than one specified price.
       Converts rows to numeric and float for calculations'''
    df.sqft = df.sqft.str.replace('sqft','').str.replace(',','').str.strip()

    df.price = df.price.str.replace('Contact', '')

    df.price = df.price.str.replace('$','').str.replace(',','').str.strip()

    df.bath = df.bath.str.replace('ba','').str.strip()

    df.bed = df.bed.str.replace('bd','').str.lower().replace('studio',0).str.strip()

    df.bed = df.bed.replace(np.nan, 0)

    df = df[~df.price.str.contains('-', na=False)]

    df.replace(' ', '', inplace=True) # whitespace to blank
    df.replace('', np.nan, inplace=True) # blank to NaN
    df.dropna(inplace=True) # drop NaN rows

    df = df.astype({'sqft': 'int32', 'price': 'int32', 'bath': 'float32', 'bed': 'float32', })
    
    return df



def save_to_csv():
    '''Saves cleaned dataframe to csv file in "daily_scrape_files" folder"'''
    logger.info('Saved scraped data to csv')
    scraped_date = str(datetime.datetime.now().date())
    scraped_page = page_url.replace('/','_').replace(',','')
    df.to_csv(f'daily_scrape_files/apartments{scraped_page}{scraped_date}.csv', index=False)



if __name__ == "__main__":
    start_time = time.time()
    logger.info('Started Scraping')    
    
    url_list = get_url_list(base_url, page_url)

    apts_data = get_all_apartments(url_list)

    df = create_df(apts_data)

    df = df_formatter(df)

    save_to_csv()
    
    elapsed_time = time.time() - start_time
    logger.info(f'Finished.  Total program time: {round(elapsed_time/60,2)} minutes')
