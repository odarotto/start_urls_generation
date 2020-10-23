from logging import Logger
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
import start_urls_generation


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


def load_publishers(publishers_path=None, file_path=None):
    """load_publishers : Reads all of the CSV files in PUBLISHERS_PATH (not checked yet) 
    and generates a list() object with several dict() objects in the following structure:

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
    if file_path is None:
        publishers = dict()
        for file_name in os.listdir(publishers_path):
            if file_name.endswith('.csv'):
                # Load the content of the CSV file
                file_path = publishers_path + '/' + file_name
                publishers[file_name.replace('.csv', '')] = load_csv_file(file_path)
        return publishers
    # ? Case of use: load publishers for only one spider
    # return load_csv_format(file_path)


def load_csv_file(file_path, url_only=False):
    spider_publishers = list()
    if os.stat(file_path).st_size != 0:
        with open(file_path, 'r') as file:
            for line in file:
                fields = line.strip().split('\t') \
                    if '\t' in line else line.strip().split()
                if url_only:
                    line_dict = fields[-1]
                    spider_publishers.append(line_dict)
                    continue
                line_dict = {
                    'company_slug': '',
                    'company_name': fields[0],
                    'start_url': fields[-1]
                } if len(fields) == 2 else {
                        'company_slug': fields[0],
                        'company_name': fields[1],
                        'start_url': fields[-1]
                    }
                spider_publishers.append(line_dict)
    return spider_publishers


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
                retry = True
                while retry:
                    if re.match(spider['start_link_regexp'], publisher_dict['start_url']):
                        param = re.match(
                            spider['start_link_regexp'], 
                            publisher_dict['start_url']
                        ).group(0)
                        if '/job/' in param:
                            param = param.split('/job/')[0]
                        to_add_dict = generate_to_add_dict(publisher_dict, param)
                        spider_start_urls.append(to_add_dict)
                        retry = False
                    # ? If the start_link_regexp is not None but we don't have a match
                    # ? process URLs further
                    else:
                        rearranged_url = rearrange_publisher_url(
                            publisher_dict['start_url'],
                            spider_name
                        )
                        retry = not publisher_dict['start_url'] == rearranged_url
                        publisher_dict['start_url'] = rearranged_url
                continue
            param = extract_domain_from_url(publisher_dict['start_url'])
            try:
                to_add_dict = generate_to_add_dict(
                    publisher_dict, 
                    spider['start_link_template'].format(param)
                )
                spider_start_urls.append(to_add_dict)
            except IndexError:
                to_add_dict = generate_to_add_dict(publisher_dict, publisher_dict['start_url'])
                spider_start_urls.append(to_add_dict)
        start_urls[spider_name] = spider_start_urls
    return start_urls


def rearrange_publisher_url(url, spider_name):
    logging.info('[!] Rearranging URL: {}'.format(url))
    # Choose the rearranger
    for f in dir(start_urls_generation):
        if spider_name in f:
            return getattr(start_urls_generation, f)(url)
    return url


def rearrange_brassring(url):
    _format = 'https://{}/TGnewUI/Search/Home/Home?partnerid={}&siteid={}#home'
    domain = extract_domain_from_url(url)
    try:
        partnerid = re.search(r'partnerid=(\d+)', url).group(1)
        siteid = re.search(r'siteid=(\d+)', url).group(1)
    except Exception:
        return url
    return _format.format(domain, partnerid, siteid)


def rearrange_ripplehire(url):
    _format = 'https://{}/ripplehire/candidate?token={}#list'
    r = requests.get(url)
    domain = extract_domain_from_url(r.url)
    try:
        token = re.search(r'token\=([a-zA-Z0-9]+)', url).group(1)
    except AttributeError:
        return url
    return _format.format(domain, token)


def rearrange_myworkday(url):
    try:
        new_url = re.search(r'(.*)(?=/job)', url).group(0)
    except Exception:
        return url
    return new_url
    

def generate_to_add_dict(publisher_dict, start_url):
    to_add_dict = {
        'company_slug': publisher_dict['company_slug'],
        'company_name': publisher_dict['company_name'],
        'start_url': start_url
    } if 'company_slug' in publisher_dict.keys() else {
        'company_name': publisher_dict['company_name'],
        'start_url': start_url
    }
    return to_add_dict


def insert_new_urls_to_repo(start_urls, comparing_publishers, from_action=''):
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
                    if 'company_slug' in in_publisher.keys():
                        del(in_publisher['company_slug'])
                    spider_new_urls.append(in_publisher)
        if len(spider_new_urls) != 0:
            # Generates a csv file for the spider if it has new urls
            df = DataFrame.from_dict(spider_new_urls)
            df.drop_duplicates(subset=None, keep='first', inplace=True)
            df.sort_index(inplace=True, ascending=False)
            folder = '/'
            if from_action == 'generate_from_google':
                folder += 'from_google/'
            elif from_action == 'generate_from_linkedin_db':
                folder += 'from_linkedin/'
            else:
                pass
            file_path = 'new_urls{}{}.csv'.format(folder, spider_name)
            df.to_csv(
                file_path, 
                sep='\t', 
                index=False, 
                index_label=False,
                header=False
            )
                

def generate_google_query(
        db_host, db_user, db_pass, db_name,
        google_include_tpl, google_ignore1_tpl, look_for=None, query=''):
    # you can generate queries only for some spiders by adding them as cmd params

    addit = ''
    if isinstance(look_for, list):
        addit = " AND `name` IN ('{}')".format("', '".join(look_for))
    if isinstance(look_for, str):
        addit = " AND `name` IN ('{}')".format(look_for)

    # Connection to db
    db = MySQLdb.connect(db_host, db_user, db_pass, db_name, use_unicode=True, charset='utf8')
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
        google_query += google_include_tpl.format(spider_domain)
        for ignored_subdomain in ignored_subdomains:
            google_query += google_ignore1_tpl.format(ignored_subdomain, spider_domain)
        if spider_name not in queries.keys():
            queries[spider_name] = list()
        queries[spider_name].append(google_query)
        spiders_names.append(spider_name)
    logging.info('[!] Queries generated for: {}'.format(', '.join(spiders_names)))
    return queries
    

def make_google_query(queries_dict, max_urls, deepnest=0):
    driver = get_chromedriver(
        # headless=True,
        user_agent=get_user_agent(),
        fast_load=True,
        images=True
    )
    js = ''

    # Load the JavaScript to generate URLs from serach results
    with open('Internet Marketing Ninjas SERP Extractor User.js') as script:
        js = script.read()
    
    spiders_results = dict()

    for spider_name, queries in queries_dict.items():
        urls = list()
        for query in queries:
            query = query.strip().replace(' ', '%20')
            try:
                get_webpage(
                    driver=driver, 
                    url='https://www.google.com/search?q={}&start={}'.format(query, deepnest),
                    wait_for_element='//div[contains(@class, "rc")]'
                )
            except Exception:
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
                time.sleep(random.uniform(0.6, 1.8) * 10)
                urls += [anchor.get_attribute('href') for anchor in driver.find_elements_by_xpath(
                    '//h2[contains(text(), "Organic Results")]/following-sibling::ol//li//a'
                )]

                # ? Check if google hide links due to repetition
                if show_repeated_results(driver, urls, max_urls):
                    continue
                # ? Check if there's a "Next" button
                if go_to_next_page(driver, urls, max_urls):
                    continue
                break
        if any(urls):
            spiders_results[spider_name] = urls
        if len(queries_dict.keys()) > 1:
            logging.info('[!] Waiting 2 minutes to extract the following spider.')
            time.sleep(120)
    driver.close()
    del(driver)
    return(spiders_results)


def go_to_next_page(driver, urls, max_urls):
    next_button = driver.find_elements_by_xpath('//a[@id="pnnext"]')
    if any(next_button) and len(urls) < max_urls:
        next_button[0].click()
        time.sleep(random.uniform(0.6, 1.8) * 10)
        return True
    return False


def show_repeated_results(driver, urls, max_urls):
    repeat_with_all_results = driver.find_elements_by_xpath(
        '//*[@id="ofr"]/i/a'
    )
    if any(repeat_with_all_results) and len(urls) < max_urls:
        repeat_with_all_results[0].click()
        time.sleep(random.uniform(0.6, 1.8) * 10)
        return True
    return False
