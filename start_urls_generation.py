from operator import index

from selenium.common.exceptions import JavascriptException
from checking_url_tool import check_urls_integrity
from os import sep
import MySQLdb
import logging
import os
import re
import time
import json
import random
import urllib.parse
from dotenv import load_dotenv, find_dotenv
from MySQLdb.cursors import DictCursor
from pandas import DataFrame
from scraping_common import *




def load_spiders_from_db(query, db_host='127.0.0.1', db_user='root', db_pass='pass', db_name='db'):
    """load_spiders_from_db :Creates a Connection() object and makes a query to the database 
    using is Cursor() object.

    Args:
        db_host (str, optional): Database Host. Defaults to '127.0.0.1'.
        db_user (str, optional): Database Username. Defaults to 'root'.
        db_pass (str, optional): Database Password. Defaults to 'pass'.
        db_name (str, optional): Database Name. Defaults to 'db'.

    Returns:
        list: list() object containing dict objects from the query.
    """

    results = list()
    db, cursor = (None, None)

    try:
        db = MySQLdb.connect(db_host, db_user, db_pass, db_name, use_unicode=True, charset='utf8')
        cursor = db.cursor(DictCursor)
        cursor.execute(query)
        results = cursor.fetchall()
    except Exception as identifier:
        logging.info('[!] Error making query to database: {}'.format(identifier))
    finally:
        if db is not None and cursor is not None:
            cursor.close()
            db.close()

    return results


def load_publishers(publishers_path):
    """load_publishers : Reads all of the CSV files in PUBLISHERS_PATH and generates a 
    list() object with several dict() objects in the following structure:

    {
        "spider_name": [
            {
                "company_slug": "slug",
                "company_name": "name",
                "start_url": "URL"
            }
        ]
    }

    Args:
        publishers_path (str): Path to the folder that contains the CSV files.

    Returns:
        list: list of dicts that represents each spider, each one of these dict objects
        contains a list with all the publishers for that spider.
    """
    # Iterate over all of the CSV files
    publishers = dict()
    for file_name in os.listdir(publishers_path):
        if file_name.endswith('.csv'):
            # Load the content of the CSV file
            file_path = publishers_path + '/' + file_name
            if os.stat(file_path).st_size != 0:
                with open(file_path, 'r') as file:
                    spiders_publishers = list()
                    for line in file:
                        fields = line.strip().split('\t') if '\t' in line else line.strip().split()
                        line_dict = {
                            'company_slug': '',
                            'company_name': fields[0],
                            'start_url': fields[-1]
                        } if len(fields) == 2 else {
                                'company_slug': fields[0],
                                'company_name': fields[1],
                                'start_url': fields[-1]
                            }
                        spiders_publishers.append(line_dict)
                    publishers[file_name.strip('.csv')] = spiders_publishers
    return publishers


def extract_domain_from_url(url):
    """extract_domain_from_url : extracts the domain part of a URL.

    Args:
        url (str): URL that we need the domain from.

    Returns:
        str: domain part of the input URL.
    """
    return urllib.parse.urlparse(url).netloc


def find_spider_by_name(name, spiders):
    """find_spider_by_name : Iterates over a list of dict objects until it find one with the name
    given as an input.

    Args:
        name (str): name of the spider we need to find.
        spiders (list): list of spiders on which the function will iterate.

    Returns:
        dict: the dict object representing the data of the spider found.
    """
    for spider in spiders:
        if name in spider['main_domain']:
            return spider


def generate_start_urls(publishers, spiders):
    """generate_start_urls : Iterates over the input publishers list, then finds the proper spider
    for the current spider_name, if the spider does not exists it continues. If the spider exists
    then iterates over all the publishers for that spider_name, matching the publishers start url 
    with the spiders start_link_regexp. If there is no match then, use the domain instead.

    Args:
        publishers (list): list of dicts with key:value like spider_name:publishers_list
        spiders (list): list of spiders

    Returns:
        dict: contains key:value pairs like spider_name:spider_start_urls 
    """
    # Extract domain from publishers URLs
    start_urls = dict()
    for spider_name, publishers_list in publishers.items():
        # Find the spider in the spiders list()
        spider = find_spider_by_name(spider_name.replace('_', '.'), spiders)
        if spider is None:
            continue

        spider_start_urls = list()
        # Iterate over the publishers list for that spider on publishers object
        for publisher_dict in publishers_list:
            # Match the raw publisher URL with the spider['start_link_regexp'] field
            param = None
            if spider['start_link_regexp'] is not None:
                if re.match(spider['start_link_regexp'], publisher_dict['start_url']):
                    param = publisher_dict['start_url']
                    if param is None:
                        print()
                    spider_start_urls.append({
                        'company_slug': publisher_dict['company_slug'],
                        'company_name': publisher_dict['company_name'],
                        'start_url': param
                    })
                    continue
            param = extract_domain_from_url(publisher_dict['start_url'])
            try:
                spider_start_urls.append({
                    'company_slug': publisher_dict['company_slug'],
                    'company_name': publisher_dict['company_name'],
                    'start_url': spider['start_link_template'].format(param)
                })
            except IndexError:
                spider_start_urls.append({
                    'company_slug': publisher_dict['company_slug'],
                    'company_name': publisher_dict['company_name'],
                    'start_url': publisher_dict['start_url']
                })
                
        start_urls[spider_name] = spider_start_urls
    return start_urls


def insert_new_urls_to_repo(start_urls, comparing_publishers):
    """insert_new_urls_to_repo : This function compares each new publisher's URL found in the input
    data against the comparing data.

    Args:
        start_urls (dict): input data
        comparing_publishers (dict): comparing data
    """
    for spider_name, publishers_list in comparing_publishers.items():
        spider_new_urls = list()
        if spider_name in start_urls.keys():
            for in_publisher in start_urls[spider_name]:
                to_add = True
                for out_publisher in publishers_list:
                    if in_publisher['start_url'] == out_publisher['start_url'] or \
                        in_publisher['start_url'] in out_publisher['start_url']:
                        to_add = False
                        break
                if to_add:
                    del(in_publisher['company_slug'])
                    spider_new_urls.append(in_publisher)
        if len(spider_new_urls) != 0:
            # Generates a csv file for the spider if it has new urls
            df = DataFrame.from_dict(spider_new_urls)
            df.drop_duplicates(subset=None, keep='first', inplace=False)
            df.sort_index(inplace=True, ascending=False)
            df.to_csv(
                    'new_urls/{}.csv'.format(spider_name), 
                    sep='\t', 
                    index=False, 
                    index_label=False,
                    header=False
                )


def generate_google_query(look_for=None, query=''):
    # you can generate queries only for some spiders by adding them as cmd params

    addit = ''
    if isinstance(look_for, list):
        addit = " AND `name` IN ('{}')".format("', '".join(look_for))
    if isinstance(look_for, str):
        addit = " AND `name` IN ('{}')".format(look_for)

    # Connection to db
    db = MySQLdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME, use_unicode=True, charset='utf8')
    cursor = db.cursor()
    sql = query.format(addit)

    cursor.execute(sql)
    lst = cursor.fetchall()
    queries = dict()
    spiders_names = list()
    for row in lst:
        spider_id = row[0]
        spider_name = row[1]

        spider_domain = row[2]
        ignored_subdomains = row[3].split(',')
        google_query = row[4]
        if not isinstance(google_query, str):
            google_query = ''
        google_query += GOOGLE_INCLUDE_TPL.format(spider_domain)
        for ignored_subdomain in ignored_subdomains:
            google_query += GOOGLE_IGNORE1_TPL.format(ignored_subdomain, spider_domain)
        if spider_name not in queries.keys():
            queries[spider_name] = list()
        queries[spider_name].append(google_query)
        spiders_names.append(spider_name)
    logging.info('[!] Queries generated for: {}'.format(', '.join(spiders_names)))
    return queries
    

def make_google_query(queries, max_urls):
    driver = get_chromedriver(
        # headless=True,
        user_agent=get_user_agent(),
        fast_load=True
    )
    js = ''

    # Load the JavaScript to generate URLs from serach results
    with open('Internet Marketing Ninjas SERP Extractor User.js') as script:
        js = script.read()
    
    spiders_results = dict()

    for spider_name, queries in queries.items():
        urls = list()
        for query in queries:
            query = query.strip().replace(' ', '%20')
            get_webpage(
                driver=driver, 
                url='https://www.google.com/search?q={}'.format(query),
                wait_for_element='//div[contains(@class, "rc")]'
            )
            while True:
                try:
                    driver.execute_script(js)
                except JavascriptException:
                    logging.info('[!] Error executing Javascript. Retrying...')
                    if 'sorry' in driver.current_url:
                        logging.info('[!] Blocked...')
                        break
                    time.sleep(random.uniform(0.6, 1.8) * 5)
                    continue
                time.sleep(random.uniform(0.6, 1.8) * 2)
                urls += [anchor.get_attribute('href') for anchor in driver.find_elements_by_xpath(
                    '//h2[contains(text(), "Organic Results")]/following-sibling::ol//li//a'
                )]

                # Check if there's a "Next" button
                next_button = driver.find_elements_by_xpath('//a[@id="pnnext"]')
                if any(next_button) and len(urls) < max_urls:
                    next_button[0].click()
                    time.sleep(random.uniform(0.6, 1.8) * 10)
                    continue
                break
        if any(urls):
            spiders_results[spider_name] = urls
    driver.close()
    del(driver)
    return(spiders_results)

                
if __name__ == "__main__":
    load_dotenv(find_dotenv())
    DB_HOST = os.getenv('DB_HOST')
    DB_USER = os.getenv('DB_USER')
    DB_PASS = os.getenv('DB_PASS')
    DB_NAME = os.getenv('DB_NAME')
    PUBLISHERS_PATH = os.getenv('PUBLISHERS_INPUT_PATH')
    PUBLISHERS_COMPARING_PATH = os.getenv('PUBLISHERS_COMPARING_PATH')
    GOOGLE_INCLUDE_TPL = ' site:*.{}'
    GOOGLE_IGNORE1_TPL = ' -site:{}.{}'
    SQL_QUERY_FOR_SPIDERS = """
        SELECT 
            `id`, 
            `name`, 
            `main_domain`, 
            `start_link_template`,
            `start_link_regexp`,
            `ignored_subdomains`, 
            `google_query` 
        FROM 
            `spiders_on_recruitnet` 
        WHERE  
            `is_ats_site`=1 AND `is_excluded`=0;
    """
    SQL_QUERY_FOR_QUERY_GENERATION = """
        SELECT 
            `id`, 
            `name`, 
            `main_domain`, 
            `ignored_subdomains`, 
            `google_query` 
        FROM 
            `spiders_on_recruitnet` 
        WHERE  
            `is_ats_site`=1 AND `is_excluded`=0{};
    """
    CHROMEDRIVER_EXE_PATH = os.getenv('CHROMEDRIVER_EXE_PATH')
    CHROMEDRIVER_COOKIES_PATH = os.getenv('CHROMEDRIVER_COOKIES_PATH')

    parser = argparse.ArgumentParser(description='Checks the state of input URLs')
    parser.add_argument(
        '--spiders', 
        metavar='S', 
        action='store', 
        type=str, 
        nargs='+', 
        default=None,
        help='One or more spider names'
    )
    parser.add_argument(
        '--max', 
        type=int, 
        action='store', 
        default=20,
        help='Max number of URLs to be collected per spider'
    )
    parser.add_argument(
        '--input_folder',
        metavar='I',
        action='store',
        type=str,
        default='',
        help='Folder path for comparing URLs. If passed as an argument then the script would not '\
        'execute the google searches'
    )
    args = parser.parse_args()

    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s')
    if args.input_folder == '':
        # Generate queries for the spiders
        queries = generate_google_query(args.spiders, SQL_QUERY_FOR_QUERY_GENERATION)

        # Perfom the queries and extract the URLs
        logging.info('[!] Perfoming Google searches.')
        spiders_urls = make_google_query(queries, max_urls=args.max)

        # Check the intregrity of the extracted URLs
        logging.info('[!] Checking URLs extracted and giving them names.')
        check_urls_integrity(spiders_urls)

    # Load input data
    logging.info('[!] Loading input URLs and repo URLs')
    spiders = load_spiders_from_db(SQL_QUERY_FOR_SPIDERS, DB_HOST, DB_USER, DB_PASS, DB_NAME)
    PUBLISHERS_PATH = PUBLISHERS_PATH if args.input_folder == '' else args.input_folder
    publishers = load_publishers(PUBLISHERS_PATH)
    comparing_publishers = load_publishers(PUBLISHERS_COMPARING_PATH)

    # Generate start_urls from input data
    logging.info('[!] Generating start URLs.')
    start_urls = generate_start_urls(publishers, spiders)

    # Compare generated start_urls with urls already in the repo
    insert_new_urls_to_repo(start_urls, comparing_publishers)
