import logging
import time
from os import sep
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


def check_urls(urls, url_template):
    
    excluded_values = []
    url_regexp = None
    url_tpl = url_template
    
    USE_SUBDOMAIN = True
    
    failed = []
    passed = []
    domains = urls.strip().split('\n')
    for domain in domains:
        go = True
        if re.search('^https?://', domain):
            if url_regexp:
                match = re.search(url_regexp, domain)
                if match:
                    domain = '|'.join(match.groups())
                else:
                    failed.append(url)
                    go = False
    
            else:
                domain = prs.urlsplit(domain).netloc.lower()
    
        if go and (not domain in passed):
            # print('>>>> {}'.format(domain))
            url = url_tpl.format(*domain.split('|'))
            success = False
            try:
                res = requests.get(url)
            except:
                res = None
                failed.append(url)

            # if res and res.text:
            #     sel_main = Selector(text=res.text)
            #     for xpath in xpaths:
            #         sel = sel_main.xpath(xpath)
            #         if sel:
            #             val = sel.get()
            #             if not val in excluded_values:
            #                 print('{}\t{}'.format(val.strip(), url))
            #                 success = True
            #                 break
            if not success:
                if USE_SUBDOMAIN:
                    print('{}\t{}'.format(subdomain_to_name(domain), url))
                else:
                    failed.append(url)
    
            passed.append(domain)
    
    
    if len(failed) > 0:
        print('====== FAILED URLS ======')
        for url in failed:
            print(url)


def check_urls_integrity(spiders_urls):
    USE_SUBDOMAIN = True
    logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s')
    
    failed = []
    passed = []
    # checked_spiders_urls = dict()
    for spider_name, urls in spiders_urls.items():
        logging.info('[!] Checking {} spider URLs.'.format(spider_name))
        checked_publisher_urls = list()
        for url in urls:
            go = False
            if re.search('^https?://', url):
                go = True
            
            if go and not url in passed:
                success = False
                res = None
                while True:
                    try:
                        logging.info('[!] Getting {}.'.format(url))
                        res = requests.get(url)
                        success = True
                    except Exception as e:
                        logging.info('[!] Catched exception:{}'.format(e))
                        time.sleep(2)
                        break
                    if res.status_code == 200:
                        logging.info('[!] Success!')
                        break
                    else:
                        logging.info('[!] Fail!')
                        break

                if res is not None:
                    if res.status_code == 200:
                        publisher = dict()
                        publisher['company_name'] = subdomain_to_name(prs.urlparse(url).netloc.lower())
                        publisher['start_url'] = url
                        checked_publisher_urls.append(publisher)
                else:
                    failed.append(url)
                passed.append(url)
        if any(checked_publisher_urls):
            pd.DataFrame.from_dict(checked_publisher_urls)\
                .to_csv(
                    'donotadd/{}.csv'.format(spider_name), 
                    sep='\t', 
                    index=False, 
                    index_label=False,
                    header=False
                )
    
    if len(failed) > 0:
        print('====== FAILED URLS ======')
        for url in failed:
            print(url)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Checks the state of input URLs')
    parser.add_argument('--urls_file_path', metavar='P', action='store', type=str)
    parser.add_argument('--url_template', metavar='U', action='store', type=str)
    args = parser.parse_args()

    check_urls(args['urls_file_path'], url_template=args['url_template'])
    