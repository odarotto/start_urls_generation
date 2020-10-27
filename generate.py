import argparse
import sys
import logging
import urllib.parse
import logging

from dotenv import load_dotenv, find_dotenv
from start_urls_generation import *


class Generate():

    def __init__(self) -> None:
        logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s')
        load_dotenv(find_dotenv())
        self.logger = logging.getLogger('GenerateLogger')
        self.DB_HOST = os.getenv('DB_HOST')
        self.DB_USER = os.getenv('DB_USER')
        self.DB_PASS = os.getenv('DB_PASS')
        self.DB_NAME = os.getenv('DB_NAME')
        self.PUBLISHERS_PATH = os.getenv('PUBLISHERS_INPUT_PATH')
        self.PUBLISHERS_COMPARING_PATH = os.getenv('PUBLISHERS_COMPARING_PATH')
        self.GOOGLE_INCLUDE_TPL = ' site:*.{}'
        self.GOOGLE_IGNORE1_TPL = ' -site:{}.{}'
        self.SQL_QUERY_FOR_SPIDERS = """
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
        self.SQL_QUERY_FOR_QUERY_GENERATION = """
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
        self.SQL_FOR_LINKEDIN_DB = """
            SELECT 
                `company_name_in_linkedin`,
                `company_domain`,
                `example_job_posting`
            FROM 
                `monitor_data` 
            WHERE 
                `is_excluded`=0 
            ORDER BY 
                `company_domain`;
        """
        self.CHROMEDRIVER_EXE_PATH = os.getenv('CHROMEDRIVER_EXE_PATH')
        self.CHROMEDRIVER_COOKIES_PATH = os.getenv('CHROMEDRIVER_COOKIES_PATH')
        logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s')
        parser = argparse.ArgumentParser(
            description='Generates URLs from Google or LinkedIn',
            usage='''
                generate.py <action> [<args>]
            '''
        )
        parser.add_argument(
            'action', 
            help='Action to execute.', 
            choices=[
                'generate_from_google',
                'generate_from_linkedin_db',
                'check_spider_urls'
            ]
        )
        self.main_args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, self.main_args.action):
            print('[!] Unrecognized command.')
            parser.print_help()
            exit(1)
        getattr(self, self.main_args.action)()


    def generate_from_google(self):
        parser = argparse.ArgumentParser(
            description='Generates Google search queries, performs them and extracts the URLs.',
            usage='generate.py generate_from_google [--spiders] [--max] [--input_folder]'
        )
        parser.add_argument(
            '--spiders', 
            metavar='S', 
            action='store', 
            type=str, 
            nargs='+', 
            default=None,
            help='One or more spider names. If not provided, spider name and query are required.'
        )
        parser.add_argument(
            '--max', 
            type=int, 
            action='store', 
            default=20,
            help='Max number of URLs to be collected per spider.'
        )
        parser.add_argument(
            '--spider_name', 
            type=str, 
            action='store', 
            default=None,
            help='Spider name to perform query for.'
        )
        parser.add_argument(
            '--query', 
            type=str, 
            action='store', 
            default=None,
            help='Query to perform the search.'
        )
        parser.add_argument(
            '--name_regex', 
            type=str, 
            action='store', 
            default=None,
            help='Variable that will help to extract the name from the URL.'
        )
        parser.add_argument(
            '--deepnest',
            type=int,
            action='store',
            default=0,
            help='Page to start looking for in the Google query.'
        )
        args = parser.parse_args(sys.argv[2:])
        # * Generate queries for the spiders
        queries_for_implemented_spiders = (
            args.query is None and args.spider_name is None
        )
        if queries_for_implemented_spiders:
            queries = generate_google_query(
                self.DB_HOST,
                self.DB_USER,
                self.DB_PASS,
                self.DB_NAME,
                self.GOOGLE_INCLUDE_TPL,
                self.GOOGLE_IGNORE1_TPL,
                look_for=args.spiders,
                query=self.SQL_QUERY_FOR_QUERY_GENERATION
            )
        elif not queries_for_implemented_spiders:
            # ? Generate a dict {spider_name: queries}
            queries = {args.spider_name: [args.query]}
        else:
            logging.error('[!] You need to provide at least one of these arguments:'\
                ' [<spiders>] or [<spider_name>] and [<query>].')
            exit(1)

        # Perfom the queries and extract the URLs
        logging.info('[!] Perfoming Google searches.')
        google_urls = make_google_query(queries, max_urls=args.max, deepnest=args.deepnest)

        # Load input data
        logging.info('[!] Loading input URLs and repo URLs')
        spiders = load_spiders_from_db(
            self.SQL_QUERY_FOR_SPIDERS, self.DB_HOST, self.DB_USER, self.DB_PASS, self.DB_NAME
        )
        logging.info('[!] Generating start URLs.')
        comparing_publishers = load_publishers(self.PUBLISHERS_COMPARING_PATH)
        start_urls = generate_start_urls(google_urls, spiders)
        for publisher in check_urls_integrity(start_urls, name_regex=args.name_regex):
            # Compare generated start_urls with urls already in the repo
            insert_new_urls_to_repo(
                publisher, 
                comparing_publishers, 
                from_action=self.main_args.action
            )


    def generate_from_linkedin_db(self):
        # ? Parser not needed until to now
        # ? DB setup
        try:
            db = MySQLdb.connect(
                self.DB_HOST,
                self.DB_USER,
                self.DB_PASS,
                self.DB_NAME,
                use_unicode=True,
                charset='utf8'
            )
            cursor = db.cursor()
            sql = self.SQL_FOR_LINKEDIN_DB
            cursor.execute(sql)
        except Exception as e:
            self.logger.info('[!] Error accesing to LinkedIn database.')
            return

        # ? Extract the required rows; 
        # ? 0:company_name_in_linkedin, 1:company_domain, 2:example_job_posting
        links = cursor.fetchall()
        urls = list()
        for row in links:
            company_name = urllib.parse.unquote(row[0]).replace('-', ' ').title()
            urls.append(
                {
                    'company_name': company_name,
                    'company_domain': row[1],
                    'example_job_posting': row[2]
                }
            )

        # ? Load active spiders
        logging.info('[!] Loading saved spiders.')
        spiders = load_spiders_from_db(
            self.SQL_QUERY_FOR_SPIDERS, self.DB_HOST, self.DB_USER, self.DB_PASS, self.DB_NAME
        )
        comparing_publishers = load_publishers(self.PUBLISHERS_COMPARING_PATH)

        # ? Organize the URLs extracted from DB
        organized_linkedin_urls_per_spider = dict()
        for spider in spiders:
            spider_publishers = list()
            for url in urls:
                if spider['main_domain'] in url['company_domain']:
                    spider_publishers.append(url['example_job_posting'])
            if any(spider_publishers):
                organized_linkedin_urls_per_spider[spider['name']] = spider_publishers
        # ? Generate start_urls from the organized URLs
        start_urls = generate_start_urls(organized_linkedin_urls_per_spider, spiders)
        insert_new_urls_to_repo(
            start_urls, 
            comparing_publishers, 
            from_action=self.main_args.action
        )


    def check_spider_urls(self):
        parser = argparse.ArgumentParser(
            description='Checks every URL in [<file_path>] looking for [<xpath>].',
            usage='generate.py check_spider_urls [xpaths] [file]'
        )
        parser.add_argument(
            '--xpaths', 
            metavar='X', 
            action='store', 
            type=str, 
            default=None,
            help='Element\'s xpath that the tool will be looking for on each URL'
        )
        parser.add_argument(
            '--file_path', 
            metavar='F', 
            action='store', 
            type=str, 
            help='File containing spider\'s URLs.'
        )
        args = parser.parse_args(sys.argv[2:])
        spider_name = args.file_path.split('/')[-1].split('.')[0]
        spider_urls = load_csv_file(file_path=args.file_path, url_only=True)
        check_xpaths = args.xpaths.split('_|_')
        check_urls_integrity({spider_name: spider_urls}, check_xpaths=check_xpaths)
        logging.info('[!] Loading input URLs and repo URLs')
        spiders = load_spiders_from_db(
            self.SQL_QUERY_FOR_SPIDERS, self.DB_HOST, self.DB_USER, self.DB_PASS, self.DB_NAME
        )
        spider_file = args.file_path.split('/')[-1]
        publishers = {spider_name: load_csv_file(self.PUBLISHERS_PATH+'/{}'.format(spider_file))}
        comparing_publishers = load_publishers(self.PUBLISHERS_COMPARING_PATH)

        # Generate start_urls from input data
        logging.info('[!] Generating start URLs.')
        start_urls = generate_start_urls(publishers, spiders)

        # Compare generated start_urls with urls already in the repo
        insert_new_urls_to_repo(start_urls, comparing_publishers, from_action=self.main_args.action)


if __name__ == "__main__":
    Generate()