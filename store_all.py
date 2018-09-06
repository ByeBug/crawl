"""
遍历json/qichacha中的文件
全部存进数据库
"""


import os
import datetime
import configparser
import logging

import pymysql
import pymongo

from store_to_mysql import store


if not os.path.isfile('myconfig.cfg'):
    print("myconfig.cfg doesn't exist")
    exit()

config = configparser.RawConfigParser()
config.read('myconfig.cfg', encoding='utf-8')

log_path = config['store']['log_path']
log_path = os.path.join(log_path, str(datetime.date.today()))
if not os.path.isdir(log_path):
    os.makedirs(log_path)

logger1 = logging.getLogger('logger1')
logger1.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
handler1 = logging.FileHandler(os.path.join(log_path, 'store_log.log'), encoding='utf-8')
handler2 = logging.FileHandler(os.path.join(log_path, 'error_log.log'), encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(message)s', '%H:%M:%S')
handler1.setFormatter(formatter)
handler2.setFormatter(formatter)
handler2.setLevel(logging.ERROR)
logger1.addHandler(stream_handler)
logger1.addHandler(handler1)
logger1.addHandler(handler2)

try:
    store_db_host = config['store_db']['host']
    store_db_port = config['store_db']['port']
    store_db_user = config['store_db']['user']
    store_db_passwd = config['store_db']['passwd']
    store_db_db = config['store_db']['db']
    conn = pymysql.connect(host=store_db_host, port=int(store_db_port), 
                            user=store_db_user, passwd=store_db_passwd, 
                            db=store_db_db, charset='utf8')
    cursor = conn.cursor()
except Exception as e:
    logger1.error('Connect to store_db failed')
    logger1.exception(e)
    exit()

try:
    crawl_mongodb_host = config['crawl_mongodb']['host']
    crawl_mongodb_port = config['crawl_mongodb']['port']
    crawl_mongodb_db = config['crawl_mongodb']['db']
    crawl_mongodb_col = config['crawl_mongodb']['collection']
    mongo_client = pymongo.MongoClient(host=crawl_mongodb_host, port=int(crawl_mongodb_port))
    mongo_collection = mongo_client[crawl_mongodb_db][crawl_mongodb_col]
except Exception as e:
    logger1.error('Connect to crawl_mongodb failed')
    logger1.exception(e)
    cursor.close()
    conn.close()
    exit()

items = mongo_collection.find({'store_time': {'$lt': '2018-09-01'}}, {'qichacha': 1, 'eastmoney': 1, 'cninfo': 1})

for item in items:
    try:
        store(item['qichacha'], item['eastmoney'], item['cninfo'], conn, cursor)
        mongo_collection.update_one({'_id': item['_id']}, {'$set': {'store_time': str(datetime.date.today())}})
        logger1.info('Store {} Successfully'.format(item['qichacha']['companyName']))
    except Exception as e:
        logger1.exception(e)
        logger1.error('Store {} Error\n'.format(item['qichacha']['companyName']))
