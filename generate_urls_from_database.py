import MySQLdb
import urllib.parse as prs
import os
import pandas as pd
from dotenv import load_dotenv, find_dotenv
# from ats_imp import check_ats_links

load_dotenv(find_dotenv())
# DB connection params
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_NAME = os.getenv('DB_NAME')
db = MySQLdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME, use_unicode=True, charset='utf8')
cursor = db.cursor()
subject = "my LinkedIn discover script"
sql = "SELECT `company_name_in_linkedin`, `example_job_posting` FROM `monitor_data` WHERE `is_excluded`=0 ORDER BY `company_domain`"
cursor.execute(sql)
links = cursor.fetchall()
urls = []
for row in links:
    # WARNING!!! don't add "wyworkday" links directly from LinkedIn
    company_domain = prs.urlsplit(row[1]).netloc.lower()
    if 'myworkday' in company_domain:
        continue
    company_name = prs.unquote(row[0]).replace('-', ' ').title()
    urls.append((company_name, row[1]))
df = pd.DataFrame.from_records(urls, columns=['company_name', 'start_url'])
print(df)
df.to_csv('from_linkedin.csv', sep='\t', index=False, index_label=False, header=True)