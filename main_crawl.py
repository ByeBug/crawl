# -*- coding: utf-8 -*-
"""
Created on Tue May 22 17:56:34 2018

@author: zhao

爬取主程序 v3.0 单线程
数据库中保存层级
股东爬两层，投资爬六层
将爬取的对象存入MongoDB
"""


import logging
import os
import random
import re
import time
import datetime
import configparser
import warnings
import signal

import pymysql
import pymongo

from spiders.crawl_qichacha import NotLoginError, NeedValidationError, crawl_from_qichacha
from spiders.crawl_stock import crawl_stock


# 读取个人配置
if not os.path.isfile('myconfig.cfg'):
    print("myconfig.cfg doesn't exist.")
    print('Please copy default.cfg to myconfig.cfg and change the content.')
    exit()

config = configparser.RawConfigParser()
config.read('myconfig.cfg', encoding='utf-8')

log_path = config['crawl']['log_path']
log_path = os.path.join(log_path, str(datetime.date.today()))
if not os.path.isdir(log_path):
    os.makedirs(log_path)

logger1 = logging.getLogger('logger1')
logger1.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
handler1 = logging.FileHandler(os.path.join(log_path, 'crawl_log.log'), encoding='utf-8')
handler2 = logging.FileHandler(os.path.join(log_path, 'error_log.log'), encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(message)s', '%H:%M:%S')
handler1.setFormatter(formatter)
handler2.setFormatter(formatter)
handler2.setLevel(logging.ERROR)
logger1.addHandler(stream_handler)
logger1.addHandler(handler1)
logger1.addHandler(handler2)

warnings.simplefilter('ignore')

logger1.info('\n=====Main Thread Start=====')

# 企业目录数据库
try:
    crawl_db_host = config['crawl_db']['host']
    crawl_db_port = int(config['crawl_db']['port'])
    crawl_db_user = config['crawl_db']['user']
    crawl_db_passwd = config['crawl_db']['passwd']
    crawl_db_db = config['crawl_db']['db']

    conn = pymysql.connect(host=crawl_db_host, port=crawl_db_port,
                        user=crawl_db_user, passwd=crawl_db_passwd,
                        db=crawl_db_db, charset='utf8')
    cursor = conn.cursor()
except Exception as e:
    logger1.error('Connect crawl_db error')
    logger1.exception(e)
    exit()

# 爬取源文件数据库
try:
    crawl_mongodb_host = config['crawl_mongodb']['host']
    crawl_mongodb_port = int(config['crawl_mongodb']['port'])
    crawl_mongodb_db = config['crawl_mongodb']['db']
    crawl_mongodb_col = config['crawl_mongodb']['collection']
    mongo_client = pymongo.MongoClient(host=crawl_mongodb_host, port=crawl_mongodb_port)
    mongo_collection = mongo_client[crawl_mongodb_db][crawl_mongodb_col]
except Exception as e:
    logger1.error('Connect crawl_mongodb error')
    logger1.exception(e)
    cursor.close()
    conn.close()
    exit()

# 终止标志
stop = False

# 信号处理函数
def signal_handler(signum, frame):
    global stop
    logger1.info('Get STOP signal')
    stop = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# 爬取人id
crawler_id = int(config['qichacha']['crawler_id'])

# 爬取延迟
crawl_delay = int(config['crawl']['delay'])

# 爬取有效期
expired = int(config['crawl']['expired'])
limit_date = datetime.date.today() - datetime.timedelta(days=expired)

wait_crawl = []
wait_write = []

need_validate = False
single_time_crawled = 0


# 写回数据库
def write_back():
    update_list = []
    insert_list = []

    for item in wait_write:
        flag, unique, name, level, crawled_date, has_error = item

        if flag == 0:
            update_list.append((crawled_date, has_error, unique))
        elif flag == 1:
            insert_list.append((unique, name, level))
    
    wait_write.clear()

    if update_list:
        sql = """UPDATE crawl 
            SET `crawled_date` = %s, `has_error` = %s
            WHERE `unique` = %s"""
        try:
            conn.ping()
            execute_num = cursor.executemany(sql, update_list)
            conn.commit()
            logger1.info('Update %d rows' % execute_num)
        except pymysql.DatabaseError as e:
            conn.rollback()
            logger1.error('Update error')
            logger1.exception(e)
    
    if insert_list:
        sql = """INSERT IGNORE crawl (`unique`, `name`, `level`)
            VALUES (%s, %s, %s)"""
        try:
            conn.ping()
            execute_num = cursor.executemany(sql, insert_list)
            conn.commit()
            logger1.info('Insert %d rows' % execute_num)
        except pymysql.DatabaseError:
            conn.rollback()
            logger1.error('Insert error')
            logger1.exception(e)


# 退出函数
def finish():
    write_back()

    cursor.close()
    conn.close()

    logger1.info('Single time crawled: {}'.format(single_time_crawled))
    logger1.info('=====Main Thread End=====\n')

    for h in logger1.handlers:
        h.close()
        logger1.removeHandler(h)

    exit()


# 根据爬取人id生成尾号列表，8返回全部尾号列表
def generate_tail(crawler_id):
    if crawler_id == 8:
        return list('0123456789abcdef')
    else:
        result = []

        result.append(hex(crawler_id * 2 + 0)[-1])
        result.append(hex(crawler_id * 2 + 1)[-1])

        return result


# 根据爬取人id生成 unique尾号
tail = generate_tail(crawler_id)


# 爬取流程
while not need_validate:

    # 获取待爬数据
    wait_crawl.clear()

    sql = """SELECT `unique`, `name`, `level` FROM crawl 
        WHERE crawled_date < %s
        AND right(`unique`, 1) IN %s
        ORDER BY level LIMIT 100"""
    try:
        conn.ping()
        cursor.execute(sql, (limit_date, tail))
    except Exception as e:
        logger1.error('Get wait crawl data error')
        logger1.exception(e)

        finish()
    
    result = cursor.fetchall()

    if result:
        wait_crawl = list(result)
        logger1.info('Put %d to wait_crawl' % len(wait_crawl))

    else:
        logger1.info('No data need crawl in database')
        
        finish()

    # 爬取数据
    for item in wait_crawl:
        # 检查是否终止程序
        if stop:
            finish()
        
        unique, name, level = item
        logger1.info('Crawling (%s)' % name)

        url = 'https://www.qichacha.com/firm_' + unique + '.html'

        # 暂时不使用代理
        proxy = None

        # 加入延时
        time.sleep(random.uniform(crawl_delay, crawl_delay + 3))

        try:
            qichacha, html = crawl_from_qichacha(name, url, proxy)

        # 出现未登录错误
        except NotLoginError as e:
            logger1.error('Not Login, Please reset cookie')
            finish()

        # 出现验证错误
        except NeedValidationError as e:
            # 等待两秒后重试
            time.sleep(2)

            try:
                qichacha = crawl_from_qichacha(name, url, proxy)

            except NeedValidationError as e:
                # 若需要验证，则该公司不需要加入待写队列

                need_validate = True

                # 清空待爬列表
                logger1.error('===!! Need Validation !!===')
                wait_crawl.clear()

                # 跳出爬取循环
                break
            
        # 爬虫出现错误
        except Exception as e:
            single_time_crawled += 1

            logger1.error('!! Crawl (%s, %s) error' % (name, url))
            logger1.exception(e)

            # 加入待写队列的item分为两类
            # 一类 flag = 0，表示需要更新该记录的 crawled_date, has_error
            # 一类 flag = 1，表示需要插入该记录的 unique, name, level

            # 更新该公司的 crawled_date 和 has_error
            flag = 0
            crawled_date = datetime.date.today()
            has_error = 1
            wait_write_item = (flag, unique, name, level, crawled_date, has_error)
            wait_write.append(wait_write_item)

            continue

        # 没有错误
        else:
            single_time_crawled += 1

            logger1.info('Crawled  (%s)' % name)

            flag = 0
            crawled_date = datetime.date.today()
            has_error = 0

            eastmoney, cninfo = '', ''
            # 若有股票代码则爬取股票信息
            if qichacha['overview']['stock_code']:
                try:
                    eastmoney, cninfo = crawl_stock(qichacha['overview']['stock_code'])
                except Exception as e:
                    # 表示该公司股票信息爬取失败
                    has_error = 2

                    logger1.error('Crawl stock %s error' % qichacha['overview']['stock_code'])
                    logger1.exception(e)

            # 爬取结果文档
            document = {
                'unique': unique,
                'company': qichacha['companyName'],
                'html': html,
                'qichacha': qichacha,
                'eastmoney': eastmoney,
                'cninfo': cninfo,
                'crawl_time': str(datetime.date.today()),
                'store_time': ''
            }

            # mongodb插入数据尝试三次
            mongodb_error = True
            for i in range(3):
                try:
                    mongo_collection.replace_one({'unique': unique}, document, upsert=True)
                    mongodb_error = False
                    break
                except pymongo.errors.AutoReconnect:
                    logger1.warning('MongoDB reconnect {} time, wait 1s'.format(i+1))
                    time.sleep(1)
                except Exception as e:
                    logger1.error('MongoDB insert (%s) error' % name)
                    logger1.exception(e)
                    break
            
            # 如果插入mongodb时出错，则放弃此公司
            if mongodb_error:
                continue

            # 更新该公司的 crawled_date 和 has_error
            wait_write_item = (flag, unique, name, level, crawled_date, has_error)
            wait_write.append(wait_write_item)

            # 根据该公司 level 将 holders 或 investments 加入待写队列

            # level = 0，根公司，将 holders 和 investments 加入待写队列
            if level == 0:
                for holder in qichacha['holders']:
                    new_unique = re.search(r'firm_(\w+).html', holder['url'])
                    if not new_unique:
                        continue
                    
                    # 插入股东信息
                    flag = 1
                    new_unique = new_unique.group(1)
                    new_name = holder['name']
                    new_level = level - 1
                    new_crawled_date = '2000-01-01'
                    new_has_error = ''
                    wait_write_item = (flag, new_unique, new_name, new_level, new_crawled_date, new_has_error)
                    wait_write.append(wait_write_item)

                for investment in qichacha['investments']:
                    new_unique = re.search(r'firm_(\w+).html', investment['url'])
                    if not new_unique:
                        continue

                    # 插入投资信息
                    flag = 1
                    new_unique = new_unique.group(1)
                    new_name = investment['name']
                    new_level = level + 1
                    new_crawled_date = '2000-01-01'
                    new_has_error = ''
                    wait_write_item = (flag, new_unique, new_name, new_level, new_crawled_date, new_has_error)
                    wait_write.append(wait_write_item)

            # -2 < level < 0，将 holders 加入待写队列
            elif -2 < level < 0:
                for holder in qichacha['holders']:
                    new_unique = re.search(r'firm_(\w+).html', holder['url'])
                    if not new_unique:
                        continue
                    
                    # 插入股东信息
                    flag = 1
                    new_unique = new_unique.group(1)
                    new_name = holder['name']
                    new_level = level - 1
                    new_crawled_date = '2000-01-01'
                    new_has_error = ''
                    wait_write_item = (flag, new_unique, new_name, new_level, new_crawled_date, new_has_error)
                    wait_write.append(wait_write_item)

            # 0 < level < 6，将 investments 加入待写队列
            elif 0 < level < 6:
                for investment in qichacha['investments']:
                    new_unique = re.search(r'firm_(\w+).html', investment['url'])
                    if not new_unique:
                        continue

                    # 插入投资信息
                    flag = 1
                    new_unique = new_unique.group(1)
                    new_name = investment['name']
                    new_level = level + 1
                    new_crawled_date = '2000-01-01'
                    new_has_error = ''
                    wait_write_item = (flag, new_unique, new_name, new_level, new_crawled_date, new_has_error)
                    wait_write.append(wait_write_item)

            # level = -2 或 level = 6，只将自身加入待写队列
            elif level == -2 or level == 6:
                # 不需要处理 holders 或 investments
                pass


    # 写回数据库
    write_back()


finish()
