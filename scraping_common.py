import pickle
import threading
import random
import datetime
import time
import csv
import requests
import time
import logging
import psycopg2
from psycopg2 import pool
from bs4 import BeautifulSoup
from sys import platform
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

logging.basicConfig(level=logging.INFO, 
    format='%(process)d-%(levelname)s-%(message)s')

def get_BeautifulSoup(url):
    '''Does a modified requests.get() and returns a BeautifulSoup object.
    '''
    session = requests.session()
    r = session.get(url, headers={'user-agent':get_user_agent()})

    return BeautifulSoup(r.text, 'lxml')
    

def create_threaded_connection(database_credentials, maxconn):
    '''Creates a ThreadedConnectionPool with the database_credentials dict()
    '''
    # Connect to database using a ThreadedConnectionPool
    logger = get_logger('Database')
    threaded_connection_pool = None
    try:
        threaded_connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=maxconn,
            user=database_credentials['user'],
            password=database_credentials['password'],
            host=database_credentials['host'],
            port=database_credentials['port'],
            database=database_credentials['database']
        )
        if threaded_connection_pool:
            logger.info('{} [!] Connected to database.'\
                .format(str(datetime.datetime.utcfromtimestamp(time.time()))))
    except (Exception, psycopg2.DatabaseError) as error:
        logger.error('[!] Error while connecting to database. ', error)
        return None
    
    return threaded_connection_pool


def get_chromedriver(executable_path=None, cookies=None, proxy=None, user_agent=None, 
                        headless=False, images=False, fast_load=False):
    """Returns a Chrome WebDriver using proxies and user-agent if specified.
    """
    drive = None
    chrome_options = webdriver.chrome.options.Options()
    if proxy is not None:
        chrome_options.add_argument('--proxy-server=%s' % proxy)
    if user_agent:
        chrome_options.add_argument('--user-agent=%s' % user_agent)
    if headless:
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('window-size=2560x1440')
        chrome_options.add_argument('--log-level=3') # Fatal
    else:
        chrome_options.add_argument('--start-maximized')
    if not images:
        chrome_prefs = dict()
        chrome_options.experimental_options["prefs"] = chrome_prefs
        chrome_prefs["profile.default_content_settings"] = {"images": 2}
        chrome_prefs["profile.managed_default_content_settings"] = {"images": 2}
    if fast_load:
        capa = DesiredCapabilities.CHROME
        capa['pageLoadStrategy'] = "none"
    else:
        capa = DesiredCapabilities.CHROME
    if cookies is not None:
        chrome_options.add_argument('--user-data-dir={}'.format(cookies))
        # chrome_options.add_experimental_option("excludeSwitches", ['enable-automation']);
    if platform == 'linux':
        driver = webdriver.Chrome(options=chrome_options)
    if platform == 'win32':
        # add the excluded argument for bypassing bot detection
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        driver = webdriver.Chrome(options=chrome_options, 
            executable_path=executable_path)
    logging.info('{} - [!] Chromedriver created.'+
        '\nProxy: {}\nUser-Agent: {}\nHeadless: {}\nLoad images: {}\nFast load: {}'\
            .format(get_formatted_time(), str(proxy), str(user_agent),
            str(headless), str(images), str(fast_load)))
    return driver


def get_user_agent():
    ua_source_url='https://deviceatlas.com/blog/list-of-user-agent-strings#desktop' 
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"\
            " Chrome/84.0.4147.105 Safari/537.36 Edg/84.0.522.52",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "\
            "Chrome/86.0.4240.183 Safari/537.36 Edg/86.0.622.63",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "\
            "Chrome/70.0.3538.77 Safari/537.36"
    ]
    return random.choice(agents)


def extract_element_data(driver, xpath, retries=3):
    data = None
    tries = 0
    extracted = False
    while not extracted:
        try:
            data = driver.find_element_by_xpath(xpath).text.strip()
            extracted = True
        except NoSuchElementException:
            time.sleep(1)
            if tries > retries:
                return "N/A"
            tries += 1
    return data


def dict_to_csv(dictionary, columns_data):
    """Saves a dictionary python object to csv format.
    """
    columns = columns_data
    
    try:
        with open('results.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            writer.writeheader()
            for data in dictionary:
                writer.writerow(data)
    except IOError as ioe:
        print('Error saving results. \n {}'.format(ioe))


def get_formatted_time():
    '''Returns a str with the current datetime.'''
    return str(datetime.datetime.utcfromtimestamp(time.time()))


def get_logger(logger_name,create_file=False):
    # create logger for prd_ci
    logging.basicConfig(level=logging.INFO, 
        name=logger_name, 
        format='%(name)s - %(levelname)s - %(message)s')
    log = logging.getLogger(logger_name)

    return log 


def save_cookies(driver, filename):
    """Extracts the cookies from a WebDriver object and saves them into a .pkl
    """
    pickle.dump(driver.get_cookies(), open(filename, 'wb'))


def load_cookies(driver, filename):
    """Loads the previously saved cookies to a new WebDriver.
    """
    try:
        cookies = pickle.load(open(filename, 'rb'))
    except FileExistsError:
        print("Cookies file not found.")
        return
    for cookie in cookies:
        driver.add_cookie(cookie)


def get_webpage(driver, url, wait_for_element=None, wait_time=10, retries=3, log_success=True):
    """Safely gets the URL with WebDriver.
    """
    tries = 0
    success = False
    while not success:
        try:
            logging.info('{} - [!] Getting {}.'.format(get_formatted_time(), url))
            driver.get(url)
            if wait_for_element is not None:
                element = WebDriverWait(driver, wait_time)\
                    .until(EC.presence_of_element_located(
                        (By.XPATH, wait_for_element)))
                logging.info('{} - [!] Element with xpath:{}. Loaded.'\
                    .format(get_formatted_time(), wait_for_element))
            if log_success:
                logging.info('{} - [!] Success getting {}'\
                    .format(get_formatted_time(), url))
            success = True
            return success
        except Exception as e:
            logging.info('{} - [!] Retrying in 3 minutes {} due to {}'\
                .format(get_formatted_time(), url, str(e)))
            if tries >= retries:
                return success
            tries += 1
            time.sleep(180)


def start_extraction_threads(in_urls, items_data, target):
    """Starts a predefined number of threads to scrape data.
    """
    threads = list()
    for _ in range(0, 3):
        x = threading.Thread(target=target,
            args=(in_urls, items_data))
        x.start()
        threads.append(x)
        time.sleep(3)
    
    return threads
