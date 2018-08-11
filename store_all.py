"""
遍历json/qichacha中的文件
全部存进数据库
"""


import datetime
import configparser

import pymysql
import pymongo

from store_to_mysql import store


config = configparser.RawConfigParser()
config.read('config.cfg', encoding='utf-8')

store_db_host = config['store_db']['host']
store_db_port = config['store_db']['port']
store_db_user = config['store_db']['user']
store_db_passwd = config['store_db']['passwd']
store_db_db = config['store_db']['db']
conn = pymysql.connect(host=store_db_host, port=store_db_port, 
                        user=store_db_user, passwd=store_db_passwd, 
                        db=store_db_db, charset='utf8')
cursor = conn.cursor()

crawl_mongodb_host = config['crawl_mongodb']['host']
crawl_mongodb_port = config['crawl_mongodb']['port']
crawl_mongodb_db = config['crawl_mongodb']['db']
crawl_mongodb_col = config['crawl_mongodb']['collection']
mongo_client = pymongo.MongoClient(host=crawl_mongodb_host, port=int(crawl_mongodb_port))
mongo_collection = mongo_client[crawl_mongodb_db][crawl_mongodb_col]

items = mongo_collection.find({'store_time': ''}, {'qichacha': 1, 'eastmoney': 1, 'cninfo': 1})

for item in items:
    store(item['qichacha'], item['eastmoney'], item['cninfo'], conn, cursor)
    
    mongo_collection.update_one({'_id': item['_id']}, {'$set': {'store_time': str(datetime.date.today())}})