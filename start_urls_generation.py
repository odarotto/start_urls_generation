import argparse
from logging import Logger
from selenium.common.exceptions import JavascriptException
from selenium.webdriver.common.action_chains import ActionChains
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
from urllib.parse import urlparse
from checking_url_tool import create_checked_dict
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


def load_publishers(publishers_path=None, file_path=None, spider=None):
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
                if spider is not None:
                    if spider in file_name:
                        file_path = publishers_path + '/' + file_name
                        return load_csv_file(file_path)
                else:
                    # Load the content of the CSV file
                    file_path = publishers_path + '/' + file_name
                    publishers[file_name.replace('.csv', '')] = load_csv_file(file_path)
        return publishers


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
                try:
                    line_dict = {
                        'company_slug': '',
                        'company_name': fields[0],
                        'start_url': fields[-1]
                    } if len(fields) == 2 else {
                        'company_slug': fields[0],
                        'company_name': fields[1],
                        'start_url': fields[-1]
                    }
                except IndexError:
                    print(fields)
                    break
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
        if name in spider['name']:
            return spider
        if name.replace('_', '.') in spider['main_domain']:
            return spider


def generate_start_urls(publishers, spiders, name_regex=None, implemented=True):
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
    if implemented:
        for spider_name, publishers_list in publishers.items():
            # Find the spider in the spiders list()
            spider = find_spider_by_name(spider_name, spiders)
            if spider is None:
                continue

            spider_start_urls = list()
            # Iterate over the publishers list for that spider on publishers object
            for publisher_url in publishers_list:
                # Match the raw publisher URL with the spider['start_link_regexp'] field
                param = None
                if spider['start_link_regexp'] is not None:
                    retry = True
                    while retry:
                        if re.match(spider['start_link_regexp'], publisher_url):
                            param = re.match(
                                spider['start_link_regexp'], 
                                publisher_url
                            ).group(0)
                            if '/job/' in param:
                                param = param.split('/job/')[0]
                            to_add_dict = generate_to_add_dict(
                                create_checked_dict(start_url=publisher_url),
                                param
                            )
                            spider_start_urls.append(to_add_dict)
                            retry = False
                        # ? If the start_link_regexp is not None but we don't have a match
                        # ? process URLs further
                        else:
                            rearranged_url = rearrange_publisher_url(
                                publisher_url,
                                spider_name
                            )
                            retry = not publisher_url == rearranged_url
                            publisher_url = rearranged_url
                    continue
                param = extract_domain_from_url(publisher_url)
                try:
                    # ? Remove repeated domains
                    start_url = spider['start_link_template'].format(param)
                    if start_url.count(spider['main_domain']) == 2:
                        start_url = re.sub(
                            re.compile(re.escape(f'.{spider["main_domain"]}')), 
                            '', 
                            start_url, 
                            count=1
                        )
                    to_add_dict = generate_to_add_dict(
                        create_checked_dict(start_url=publisher_url),
                        start_url
                    )
                    spider_start_urls.append(to_add_dict)
                except IndexError:
                    to_add_dict = generate_to_add_dict(
                        create_checked_dict(start_url=publisher_url), 
                        publisher_url
                    )
                    spider_start_urls.append(publisher_url)
            start_urls[spider_name] = spider_start_urls
        return start_urls
    logging.info('[!] Generating start URLs for an URL not implemented yet.')
    for spider_name, publishers_list in publishers.items():
        spider_start_urls = list()
        for publisher_url in publishers_list:
            publisher_url = rearrange_publisher_url(publisher_url, spider_name)
            to_add_dict = generate_to_add_dict(
                create_checked_dict(start_url=publisher_url), 
                publisher_url
            )
            spider_start_urls.append(to_add_dict)
        start_urls[spider_name] = spider_start_urls
    return start_urls


def generate_live_link(spider, publisher):
    _format = 'https://global.recruit.net/search.html?query=site%3Afeed{}{}&location=&s='
    clean_spider_name = re.sub(r'[^a-zA-Z]', '', spider.lower())
    clean_publisher_name = re.sub(r'[^a-zA-Z]', '', publisher.lower())
    return _format.format(clean_publisher_name, clean_spider_name)


def rearrange_publisher_url(url, spider_name):
    logging.info('[!] Rearranging URL: {}'.format(url))
    # Choose the rearranger
    packages = dir(start_urls_generation)
    for f in packages:
        if spider_name in f:
            new_url = getattr(start_urls_generation, f)(url)
            logging.info('[!] Rearranged to: {}'.format(new_url))
            return new_url
    return url


def rearrange_current_vacancies(url):
    _format = 'https://{}/Careers/SearchVacancies'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_lever_co(url):
    _format = 'https://jobs.lever.co{}'
    parsed = urlparse(url)
    return _format.format(parsed.path)


def rearrange_talentclue(url):
    # _format = 'https://{}/res_joblist.html'
    # parsed = urlparse(url)
    if '?' in url:
        url = url.split('?')[0]
    return url


def rearrange_peoplefluent(url):
    _format = 'https://{}/res_joblist.html'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_selecty_com_br(url):
    _format = 'https://{}/search'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_avature_net(url):
    _format = 'https://{}/careers/SearchJobs'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)

def rearrange_candidatecare_jobs(url):
    _format = 'https://{}/job_positions/browse'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_hr_technologies(url):
    _format = 'https://{}/content/jobpage.asp'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_altamiraweb(url):
    _format = 'https://{}'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_tool2match_nl(url):
    _format = 'https://{}/api/jobs/published'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_teamtailor(url):
    _format = 'https://{}/jobs'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_talentsoft_com(url):
    _format = 'https://{}/offre-de-emploi/liste-toutes-offres.aspx?all=1'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_candidats_talentsin_com(url):
    _format = 'https://{}/update-jobs-list?page=1'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_selectminds(url):
    _format = 'https://{}/jobs/search'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_turborecruit_com_au(url):
    _format = 'https://{}/job/job_search_result.cfm'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_interviewexchange(url):
    _format = 'https://{}/jobsrchresults.jsp?Job_State0=*&New_Job_Post=0&Cat_Id0=*'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_mercury_com_au(url):
    _format = 'https://{}/SearchResults.aspx'
    parsed = urlparse(url)
    return _format.format(parsed.netloc)


def rearrange_jobdiva(url):
    _format = 'https://www1.jobdiva.com/private/myjobs/searchjobsdone_outside.jsp?a={}'
    try:
        publiser = re.search(r'a=([a-zA-Z0-9]+)', url).group(1)
        new_url = _format.format(publiser)
    except Exception as e:
        return url
    return new_url


def rearrange_greenhouse(url):
    _format = 'https://boards.greenhouse.io/embed/job_board?for={}'
    try:
        publisher = re.search(r'\.io\/(\w+)', url).group(1)
        new_url = _format.format(publisher)
    except Exception:
        return url
    return new_url


def rearrange_applitrack(url):
    try:
        new_url = url.split('?')[0] + '?all=1&embed=1'
    except Exception:
        return url
    return new_url


def rearrange_human_sourcing(url):
    _format = 'https://{}/en'
    try:
        parsed = urlparse(url)
        new_url = _format.format(parsed.netloc)
    except Exception:
        return url
    return new_url


def rearrange_brassring(url):
    _format = 'https://{}/TGnewUI/Search/Home/Home?partnerid={}&siteid={}#home'
    domain = extract_domain_from_url(url)
    partnerid = None
    siteid = None
    try:
        partnerid = re.search(r'partnerid=(\d+)', url).group(1)
        siteid = re.search(r'siteid=(\d+)', url).group(1)
    except Exception:
        return url
    new_url = _format.format(domain, partnerid, siteid)
    return new_url


def rearrange_prevueaps_ca(url):
    _format = 'https://{}/jobs/'
    new_url = _format.format(urlparse(url).netloc)
    return new_url


def rearrange_ripplehire(url):
    _format = 'https://{}/ripplehire/candidate?token={}#list'
    try:
        r = requests.get(url)
    except Exception:
        return url
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


def rearrange_hirehive_com(url):
    _format = 'https://{}.hirehive.com/'
    parsed_url = urlparse(url)
    return _format.format(parsed_url.path.strip('/'))
    

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


def insert_new_urls_to_repo(start_urls, comparing_publishers, from_action='', implemented=True):
    """insert_new_urls_to_repo : This function compares each new publisher's URL found in the input
    data against the comparing data.

    Args:
        start_urls (dict): input data
        comparing_publishers (dict): comparing data
    """
    if implemented:
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
                df.drop_duplicates(subset='start_url', keep='first', inplace=True)
                df.sort_values(inplace=True, by=['company_name'])
                folder = '/'
                if from_action == 'generate_from_google':
                    folder += 'from_google/'
                elif from_action == 'generate_from_linkedin_db':
                    folder += 'from_linkedin/'
                else:
                    folder += 'from_google/'
                file_path = 'new_urls{}{}.csv'.format(folder, spider_name)
                logging.info('[!] Saving {} to {}'.format(spider_name, folder))
                df.to_csv(
                    file_path, 
                    sep='\t', 
                    index=False, 
                    index_label=False,
                    header=False
                )
        return True
    else:
        # save start_urls
        spider_name = list(start_urls.keys())[0]
        df = DataFrame.from_records(start_urls[spider_name])
        df.drop_duplicates(subset='start_url', keep='first', inplace=True)
        df.sort_values(inplace=True, by=['company_name'])
        folder = '/'
        if from_action == 'generate_from_google':
            folder += 'from_google/'
        elif from_action == 'generate_from_linkedin_db':
            folder += 'from_linkedin/'
        else:
            folder += 'from_google/'
        file_path = 'new_urls{}{}.csv'.format(folder, spider_name)
        logging.info('[!] Saving {} to {}'.format(spider_name, folder))
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
        if 'site' not in google_query:
            google_query += google_include_tpl.format(spider_domain)
            for ignored_subdomain in ignored_subdomains:
                google_query += google_ignore1_tpl.format(ignored_subdomain, spider_domain)
        if spider_name not in queries.keys():
            queries[spider_name] = list()
        queries[spider_name].append(google_query)
        spiders_names.append(spider_name)
    logging.info('[!] Queries generated for: {}'.format(', '.join(spiders_names)))
    return queries
    

def make_google_query(queries_dict: dict, max_urls: int, deepnest: int=0) -> dict:
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
                    wait_for_element='//div[@id="search"]//div[@class="g"]',
                    log_success=False
                )
            except Exception:
                get_webpage(
                    driver=driver,
                    url='https://www.google.com/search?q={}'.format(query),
                    wait_for_element='//div[@id="search"]//div[@class="g"]',
                    log_success=False
                )

            # # ? Change to english
            # english_link = driver.find_element_by_xpath('//a[contains(text(), "Change to English")]')
            # ActionChains(driver).move_to_element(english_link).click().perform()
            # time.sleep(1)

            # # ? Select the time interval
            # tools_button = driver.find_element_by_xpath('//div[contains(text(), "Tools")]') # ActionChains(driver).move_to_element(tools_button).click().perform()
            # ActionChains(driver).move_to_element(tools_button).click().perform()
            # time.sleep(1)
            # interval_button = driver.find_element_by_xpath('//div[contains(text(), "Any time")]')
            # ActionChains(driver).move_to_element(interval_button).click().perform()
            # time.sleep(1)
            # past_month_button = driver.find_element_by_xpath('//a[contains(text(), "Past 24 hours")]')
            # ActionChains(driver).move_to_element(past_month_button).click().perform()
            # time.sleep(1)

            # for index, row in df.iterrows():
            #     if '?' in row['url']:
            #         row['name'] = re.search(r'co\/([^\?]+)', row['url']).group(1).capitalize()
            #         publisher = re.search(r'co\/([^\?]+)', row['url']).group(1)
            #         row['url'] = f'https://jobs.lever.co/{publisher}'
            #         continue
            #     row['name'] = re.search(r'co\/(.+)', row['url']).group(1).capitalize()

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
                time.sleep(random.uniform(0.6, 1.8) * 15)
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
            logging.info(f'[!] {len(urls)} URL{"s" if len(urls) > 1 else ""} extracted.')
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
        time.sleep(random.uniform(0.6, 1.8) * 5)
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
