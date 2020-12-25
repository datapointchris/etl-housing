import datetime
import functools
import logging
import logging.handlers
import os
import time

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import NoSuchElementException
from tqdm import tqdm

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s : %(name)s : %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
file_handler = logging.handlers.RotatingFileHandler('logs/scraper.log', maxBytes=10485760, backupCount=12)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


# ========== SETTINGS ========== #
SAVE_DIRECTORY = 'daily_scrape'

CITIES = ['Woburn,MA']
# , 'Cambridge,MA', 'Sommerville,MA', 'Brookline,MA',
#   'Allston,MA', 'Watertown,MA', 'Waltham,MA', 'Newton,MA', 'Medford,MA',
#   'Belmont,MA', 'Arlington,MA', 'Malden,MA', 'Everett,MA', 'Brighton,MA']

# headless browser
firefox_options = Options()
firefox_options.headless = True


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


class CityScraper:
    """Scrapes a city for apartment listings."""

    def __init__(self, city, save_directory=SAVE_DIRECTORY, webdriver=None):
        self.city = city
        self.city_url = f'https://www.trulia.com/for_rent/{city}'
        self.browser = webdriver

    def _set_browser(self):

        return browser

    def browser_get(self, url):
        """ Gets the url specified.  Contains error handling"""
        try:
            self.browser.get(url)
        except (ConnectionError, ConnectionResetError) as e:
            logger.info(f'Error {e} for URL: {url}')

    def get_list_page_urls(self):
        """Gets the urls on the list page"""
        listing_urls = []
        for elem in self.browser.find_elements_by_class_name('jLNYlr'):
            try:
                listing_urls.append(elem.find_element_by_tag_name('a').get_attribute('href'))
            except NoSuchElementException:
                continue
        return listing_urls

    def get_next_page(self):
        """ Gets next page link and returns flag indicating if last page"""
        try:
            soup = BeautifulSoup(self.browser.page_source, 'html.parser')
            href_suffix = soup.find('a', {'aria-label': 'Next Page'})['href']
            next_page = 'https://www.trulia.com' + href_suffix
            last_page = False
            return next_page, last_page
        except TypeError:
            next_page = None
            last_page = True
            return next_page, last_page

    def get_apartment_urls_for_city(self):
        '''Gets a list of urls for city from all listing pages'''
        i = 1
        url_list = []
        next_page = self.city_url
        last_page = False
        while last_page is False:
            print(f'Page {i}, Total Apartment URLs: {len(url_list)}') if i % 10 == 0 else None
            self.browser_get(next_page)

            list_page_urls = self.get_list_page_urls()
            url_list.extend(list_page_urls)
            next_page, last_page = self.get_next_page()
            i += 1
            time.sleep(.05)
        return url_list

    def get_apartment_data(self, url):
        '''Gets apartment data for the url specified'''
        self.browser_get(url)
        content = self.browser.page_source
        soup = BeautifulSoup(content, 'html.parser')

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
                    'li', {'class': 'FeatureList__FeatureListItem-iipbki-0'}
                )]
                details = ' ,'.join(details)

                apartment_url = url
                date = str(datetime.datetime.now().date())
                apartment_list.append([name, address, unit, sqft, bed, bath, price, city,
                                       state, zipcode, description, details, apartment_url, date])
        return apartment_list

    def create_apartment_df(self, data):
        df = pd.DataFrame(data, columns=['name', 'address', 'unit', 'sqft', 'bed', 'bath', 'price',
                                         'city', 'state', 'zipcode', 'description', 'details', 'url', 'date'])
        return df

    def clean_apartment_df(self, df):
        '''Formats the dataframe to remove special characters, spaces, and NaN values.
        Removes units with price ranges, rather than one specified price.'''

        df.sqft = df.sqft.str.replace('sqft', '').str.replace(',', '').str.strip()
        df = df.loc[df.sqft != '']  # throw out empty square foot apts
        mask = df.sqft.str.contains('-')
        df.loc[mask, 'sqft'] = df.loc[mask, 'sqft'].apply(lambda x: np.mean(list(map(int, x.split('-')))))
        df.price = df.price.str.replace('Contact', '')
        df.price = df.price.str.replace('$', '').str.replace(',', '').str.replace('+', '').str.strip()
        df.bath = df.bath.str.replace('ba', '').str.strip()
        df.bed = df.bed.str.replace('bd', '').str.lower().replace('studio', 0).str.strip()
        df.bed = df.bed.replace(np.nan, 0)
        df = df[~df.price.str.contains('-', na=False)]  # throw out price range apts
        df.replace(' ', '', inplace=True)  # whitespace to blank
        df.replace('', np.nan, inplace=True)  # blank to NaN
        # df.dropna(inplace=True)  # drop NaN rows

        return df

    def convert_df_columns(self, df):
        '''Converts rows to numeric and float for calculations'''
        df = df.astype({'sqft': 'int32', 'price': 'int32', 'bath': 'float32',
                        'bed': 'float32', 'zipcode': 'int32'})
        return df

    def save_to_csv(self, df):
        '''Saves cleaned dataframe to csv file in "daily_scrape_files" folder"'''
        scraped_date = str(datetime.datetime.now().date())
        city_name = self.city.replace(',', '_')
        filedir = f'{SAVE_DIRECTORY}/{city_name}'
        os.makedirs(filedir, exist_ok=True)
        df.to_csv(f'{filedir}/{scraped_date}.csv', index=False)


@function_timer
def main():

    for city in CITIES:
        with webdriver.Firefox(options=firefox_options) as driver:
            logger.info(f'START SCRAPE: {city}')
            scraper = CityScraper(city, save_directory=SAVE_DIRECTORY, webdriver=driver)
            logger.info('Getting URL list')
            apartment_url_list = scraper.get_apartment_urls_for_city()
            logger.info(f'URLs retrieved: {len(apartment_url_list)}')

            logger.info('Getting apartment data from url_list')
            apartments_data = []
            for i, apartment_url in enumerate(tqdm(apartment_url_list), start=1):
                logger.info(f'URL {i} of {len(apartment_url_list)}') if i % 500 == 0 else None
                try:
                    apt_data = scraper.get_apartment_data(apartment_url)
                    apartments_data.extend(apt_data)
                except Exception as e:
                    logger.info(f'Exception getting apartment data for url {apartment_url}', e)
                    continue
            logger.info(f'Apartments retrieved: {len(apartments_data)}')

            apartment_df = scraper.create_apartment_df(apartments_data)
            cleaned_apartment_df = scraper.clean_apartment_df(apartment_df)
            cleaned_and_converted_apartment_df = scraper.convert_df_columns(cleaned_apartment_df)
            scraper.save_to_csv(cleaned_and_converted_apartment_df)
            scraper.browser.quit()
            logger.info(f'FINISH SCRAPE: {city}')


if __name__ == "__main__":
    main()
