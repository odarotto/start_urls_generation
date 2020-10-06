import MySQLdb, sys

# you can generate queries only for some spiders by adding them as cmd params
spiders = []
if len(sys.argv) > 1:
    i = 0
    for param in sys.argv:
        if i > 0:
            spiders.append(param)
        i += 1

addit = ''
if len(spiders) > 0:
    addit = " AND `name` IN ('{}')".format("', '".join(spiders))

# DB connection params
DB_HOST = '127.0.0.1'
DB_USER = 'root'
DB_PASS = 'Alcala$250595'
DB_NAME = 'li_jobsites_intelligence'

db = MySQLdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME, use_unicode=True, charset='utf8')
cursor = db.cursor()

GOOGLE_INCLUDE_TPL = ' site:*.{}'
GOOGLE_IGNORE1_TPL = ' -site:{}.{}'
sql = "SELECT `id`, `name`, `main_domain`, `ignored_subdomains`, `google_query` \
        FROM `spiders_on_recruitnet` WHERE  `is_ats_site`=1 AND `is_excluded`=0{};".format(addit)

cursor.execute(sql)
lst = cursor.fetchall()
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
    print('======================================================')
    print(google_query)
    print('======================================================')

