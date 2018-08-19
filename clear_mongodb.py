"""
清理 mongodb 中重复的数据
"""


import configparser

import pymongo


config = configparser.RawConfigParser()
config.read('config.cfg', encoding='utf-8')

crawl_mongodb_host = config['crawl_mongodb']['host']
crawl_mongodb_port = int(config['crawl_mongodb']['port'])
crawl_mongodb_db = config['crawl_mongodb']['db']
crawl_mongodb_col = config['crawl_mongodb']['collection']
mongo_client = pymongo.MongoClient(host=crawl_mongodb_host, port=crawl_mongodb_port)
mongo_collection = mongo_client[crawl_mongodb_db][crawl_mongodb_col]

uniques = mongo_collection.aggregate([
    {'$group': {'_id': '$unique', 'company': {'$first': '$company'}, 'num': {'$sum': 1}}},
    {'$match': {'num': {'$gt': 1}}},
    {'$sort': {'num': -1}}
])

for i in uniques:
    u = i['_id']
    company = i['company']
    num = i['num']

    q = mongo_collection.aggregate([
        {'$project': {'unique': 1, 'company': 1, 'crawl_time': 1}},
        {'$match': {'unique': u}},
        {'$sort': {'crawl_time': -1}},
        {'$skip': 1}
    ])

    print('Deleting %s, %d' % (company, num))
    for j in q:
        mongo_id = j['_id']
        mongo_collection.delete_one({'_id': mongo_id})
