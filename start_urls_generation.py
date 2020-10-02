import MySQLdb
import logging
import os
import re
import json
import urllib.parse
from dotenv import load_dotenv, find_dotenv

from MySQLdb.cursors import DictCursor
from dotenv import main


load_dotenv(find_dotenv())
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_NAME = os.getenv('DB_NAME')
PUBLISHERS_PATH = os.getenv('PUBLISHERS_INPUT_PATH')
PUBLISHERS_COMPARING_PATH = os.getenv('PUBLISHERS_COMPARING_PATH')
GOOGLE_INCLUDE_TPL = ' site:*.{}'
GOOGLE_IGNORE1_TPL = ' -site:{}.{}'
SQL_QUERY = """
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
                        fields = line.strip().split('\t')
                        line_dict = {
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
            spider_start_urls.append({
                'company_slug': publisher_dict['company_slug'],
                'company_name': publisher_dict['company_name'],
                'start_url': spider['start_link_template'].format(param)
            })
                
        start_urls[spider_name] = spider_start_urls
    return start_urls


def insert_new_urls_to_repo(start_urls, comparing_publishers):
    for spider_name, publishers_list in comparing_publishers.items():
        if spider_name in start_urls.keys():
            for in_publisher in start_urls[spider_name]:
                to_add = True
                for out_publisher in publishers_list:
                    if in_publisher == out_publisher:
                        to_add = False
                        break
                if to_add:
                    print(
                        '[!] Publisher: {}\n can be added to {} spider.'
                            .format(in_publisher, spider_name)
                    )
        continue


if __name__ == "__main__":
    # Load input data
    spiders = load_spiders_from_db(SQL_QUERY, DB_HOST, DB_USER, DB_PASS, DB_NAME)
    publishers = load_publishers(PUBLISHERS_PATH)
    comparing_publishers = load_publishers(PUBLISHERS_COMPARING_PATH)

    # Generate start_urls from input data
    start_urls = generate_start_urls(publishers, spiders)

    # Compare generated start_urls with urls already in the repo
    insert_new_urls_to_repo(start_urls, comparing_publishers)
