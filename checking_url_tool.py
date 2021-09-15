import logging
from scraping_common import get_user_agent
import time
from os import name, sep
import re, requests
import urllib.parse as prs
import argparse
import pandas as pd
from scrapy import Selector

def subdomain_to_name(domain):
    a = domain.split('|')
    arr = a[0].split('.')
    name = str(arr[0]).strip()
    suff = ''
    if len(a) > 1:
        a.pop(0)
        suff = ' ' + ' '.join(a).capitalize()
    if len(name) < 5:
        return name.upper() + suff
    else:
        return name.capitalize() + suff


def check_urls_integrity(spiders_urls, check_xpaths=None, name_regex=None):
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s')
    
    failed = []
    passed = []
    # checked_spiders_urls = dict()
    for spider_name, publishers in spiders_urls.items():
        logging.info('[!] Checking {} spider URLs.'.format(spider_name))
        checked_publisher_urls = list()
        for publisher in publishers:
            go = False
            if re.search('^https?://', publisher['start_url']):
                go = True
            
            if go and not publisher['start_url'] in passed:
                res = None
                try:
                    headers = {'user-agent': get_user_agent()}
                    res = requests.get(publisher['start_url'], timeout=30, headers=headers)
                except Exception as e:
                    logging.info(
                        '[!] Catched exception on {}:\n{}'.format(publisher['start_url'], e)
                    )
                    time.sleep(2)

                if res is not None and res.status_code == 200:
                    logging.info('[!] {} -- {}.'.format(publisher['start_url'], res.status_code))
                    if check_xpaths is not None:
                        sel_main = Selector(text=res.text)
                        xpaths_found = any(
                            [
                                sel_main.xpath(xpath).get() is not None for xpath in check_xpaths
                            ]
                        )
                        if xpaths_found:
                            checked_publisher_urls.append(
                                create_checked_dict(
                                    res, 
                                    publisher['start_url'], 
                                    name_regex=name_regex
                                )
                            )
                            logging.info('[!] Success!')
                            passed.append(publisher['start_url'])
                        else:
                            logging.info('[!] Xpath was not found!')
                            checked_publisher_urls.append(
                                create_checked_dict(
                                    res, 
                                    publisher['start_url'], 
                                    result='XPATH not found', 
                                    name_regex=name_regex
                                )
                            )
                            failed.append(publisher['start_url'])
                            passed.append(publisher['start_url'])
                    checked_publisher_urls.append(
                        create_checked_dict(
                            res, 
                            publisher['start_url'], 
                            result='', 
                            name_regex=name_regex
                        )
                    )
                    passed.append(publisher['start_url'])
                else:
                    if res is not None:
                        logging.info(
                            '[!] {} -- {}.'.format(publisher['start_url'], res.status_code)
                        )
                    else:
                        logging.info('[!] {} -- failed.'.format(publisher['start_url']))
                    failed.append(publisher['start_url'])
                    passed.append(publisher['start_url'])
        if any(checked_publisher_urls):
            yield {spider_name: checked_publisher_urls}
    
    if len(failed) > 0:
        print('====== FAILED URLS ======')
        for publisher in failed:
            print(publisher)

def create_checked_dict(request=None, start_url=None, result=None, name_regex=None):
    publisher = dict()
    if name_regex is None:
        publisher['company_name'] = subdomain_to_name(
            prs.urlparse(start_url.replace('www.', '')).netloc.lower()
        ).replace('-', '').title()
    else:
        try:
            publisher['company_name'] = re.search(re.compile(name_regex), start_url).group(1)
        except AttributeError:
            publisher['company_name'] = subdomain_to_name(
                prs.urlparse(start_url.replace('www.', '')).netloc.lower()
            )
    publisher['start_url'] = start_url
    # publisher['result'] = request.status_code if result is None else result
    return publisher


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Checks the state of input URLs')
    parser.add_argument('--urls_file_path', metavar='P', action='store', type=str)
    parser.add_argument('--url_template', metavar='U', action='store', type=str)
    args = parser.parse_args()

    check_urls(args['urls_file_path'], url_template=args['url_template'])
    