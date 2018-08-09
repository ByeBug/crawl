# -*- coding: utf-8 -*-
"""
Created on Tue May 22 17:56:34 2018

@author: zhao

多线程爬取主程序 v2.0
数据库中保存层级
股东爬两层，投资爬六层
"""


import logging
import os
import queue
import random
import re
import threading
import time
import datetime
import configparser

import pymysql

from spiders.crawl_qichacha import NeedValidationError, crawl_from_qichacha
from spiders.crawl_stock import crawl_stock


config = configparser.RawConfigParser()
config.read('config.cfg', encoding='utf-8')

log_path = config['crawl']['log_path']
log_path = os.path.join(log_path, str(datetime.date.today()))
if not os.path.isdir(log_path):
    os.makedirs(log_path)

logger1 = logging.getLogger('logger1')
logger1.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
handler1 = logging.FileHandler(os.path.join(log_path, 'crawl_log.log'), mode='w', encoding='utf-8')
handler2 = logging.FileHandler(os.path.join(log_path, 'error_log.log'), mode='w', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(message)s', '%H:%M:%S')
handler1.setFormatter(formatter)
handler2.setFormatter(formatter)
handler2.setLevel(logging.ERROR)
logger1.addHandler(stream_handler)
logger1.addHandler(handler1)
logger1.addHandler(handler2)

crawl_db_user = config['crawl_db']['user']
crawl_db_passwd = config['crawl_db']['passwd']
crawl_db_db = config['crawl_db']['db']
conn = pymysql.connect(host='localhost', port=3306,
                       user=crawl_db_user, passwd=crawl_db_passwd,
                       db=crawl_db_db, charset='utf8')
cursor = conn.cursor()

wait_crawl_q = queue.Queue()
wait_write_q = queue.Queue()

db_semaphore = threading.Semaphore(1)

need_validate = False
single_time_crawled = 0


def crawl_stock_wrapper(code):
    try:
        crawl_stock(code)
    except Exception as e:
        logger1.exception(e)
        logger1.error('crawl stock {} error'.format(code))


class ReadThread(threading.Thread):
    def __init__(self, cursor, wait_crawl_q, db_semaphore):
        super().__init__()
        self.cursor = cursor
        self.wait_crawl_q = wait_crawl_q
        self.db_semaphore = db_semaphore

    def run(self):
        # 爬取有效期为 30 天
        limit_date = datetime.date.today() - datetime.timedelta(days=30)

        while 1:
            time.sleep(2)
            sql = """select `unique`, `name`, `level` from crawl 
                where crawled_date < %s
                order by level limit 100"""
            self.db_semaphore.acquire()
            self.cursor.execute(sql, (limit_date, ))
            result = self.cursor.fetchall()
            self.db_semaphore.release()

            if result:
                for row in result:
                    unique = row[0]
                    name = row[1]
                    level = row[2]

                    self.wait_crawl_q.put((unique, name, level))

                logger1.info('----- ReadThread blocking')
                self.wait_crawl_q.join()

                logger1.info('----- ReadThread Wake up')
                if need_validate:
                    logger1.info('-----ReadThread: Need validate')
                    logger1.info('-----ReadThread end-----')
                    return
            else:
                logger1.info('-----ReadThread: No data need crawl in database')
                logger1.info('-----ReadThread end-----')
                return


class CrawlThread(threading.Thread):
    def __init__(self, thread_name, wait_crawl_q, wait_write_q):
        super().__init__()
        self.thread_name = thread_name
        self.wait_crawl_q = wait_crawl_q
        self.wait_write_q = wait_write_q

    def run(self):
        global single_time_crawled, need_validate

        while 1:
            try:
                # 若待爬队列15s无数据
                unique, name, level = self.wait_crawl_q.get(timeout=15)

                url = 'https://www.qichacha.com/firm_' + unique + '.html'

                # 暂时不使用代理
                proxy = None

                # 加入延时
                time.sleep(random.uniform(1, 2))

                try:
                    qichacha = crawl_from_qichacha(name, url, proxy)

                # 出现验证错误
                except NeedValidationError as e:
                    # 等待两秒后重试
                    time.sleep(2)

                    try:
                        qichacha = crawl_from_qichacha(name, url, proxy)
                    except NeedValidationError as e:
                        # 若需要验证，则该公司不需要加入待写队列

                        self.wait_crawl_q.task_done()

                        need_validate = True

                        # 清空待爬队列
                        logger1.error('===!!{} get Need Validation Error, clearing wait_crawl_q'.format(self.thread_name))
                        while not self.wait_crawl_q.empty():
                            try:
                                self.wait_crawl_q.get_nowait()
                                self.wait_crawl_q.task_done()
                            except queue.Empty:
                                logger1.error('!!!!!{} get Empty Error when clear wait_crawl_q'.format(self.thread_name))
                        logger1.error('===!!{} clear wait_crawl_q finished'.format(self.thread_name))

                        continue
                
                # 爬虫出现错误
                except Exception as e:
                    logger1.error('!!!!!{} crawl ({}, {}) error!!!!!'.format(self.thread_name, name, url))
                    logger1.exception(e)

                    # 加入待写队列的item分为两类
                    # 一类 flag = 0，表示需要更新该记录的 crawled_date, has_error
                    # 一类 flag = 1，表示需要插入该记录的 unique, name, level

                    # 更新该公司的 crawled_date 和 has_error
                    flag = 0
                    crawled_date = datetime.date.today()
                    has_error = 1
                    wait_write_q_item = (flag, unique, name, level, crawled_date, has_error)
                    self.wait_write_q.put(wait_write_q_item)

                    self.wait_crawl_q.task_done()

                    single_time_crawled += 1

                    continue

                # 没有错误
                else:
                    logger1.info('+++++{} crawled ({})'.format(self.thread_name, name))

                    # 更新该公司的 crawled_date 和 has_error
                    flag = 0
                    crawled_date = datetime.date.today()
                    has_error = 0
                    wait_write_q_item = (flag, unique, name, level, crawled_date, has_error)
                    self.wait_write_q.put(wait_write_q_item)

                    # 若有股票代码则爬取股票信息
                    if qichacha['overview']['stock_code']:
                        crawl_stock_thread = threading.Thread(target=crawl_stock_wrapper, args=(qichacha['overview']['stock_code'], ), name='crawl-stock-thread')
                        crawl_stock_thread.start()

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
                            wait_write_q_item = (flag, new_unique, new_name, new_level, new_crawled_date, new_has_error)
                            self.wait_write_q.put(wait_write_q_item)

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
                            wait_write_q_item = (flag, new_unique, new_name, new_level, new_crawled_date, new_has_error)
                            self.wait_write_q.put(wait_write_q_item)

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
                            wait_write_q_item = (flag, new_unique, new_name, new_level, new_crawled_date, new_has_error)
                            self.wait_write_q.put(wait_write_q_item)

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
                            wait_write_q_item = (flag, new_unique, new_name, new_level, new_crawled_date, new_has_error)
                            self.wait_write_q.put(wait_write_q_item)

                    # level = -2 或 level = 6，只将自身加入待写队列
                    elif level == -2 or level == 6:
                        # 不需要处理 holders 或 investments
                        pass
        
                self.wait_crawl_q.task_done()

                single_time_crawled += 1

            except queue.Empty:
                logger1.info('+++++{}: No data in wait_crawl_q'.format(self.thread_name))
                logger1.info('+++++{} end+++++'.format(self.thread_name))

                return


class WriteThred(threading.Thread):
    def __init__(self, conn, cursor, wait_write_q, db_semaphore):
        super().__init__()
        self.conn = conn
        self.cursor = cursor
        self.wait_write_q = wait_write_q
        self.db_semaphore = db_semaphore

    def run(self):
        # 每条单独写入 or 批量写入？

        # # 单条写入
        # while 1:
        #     try:
        #         # 若待写队列20s没有数据
        #         flag, unique, name, level, crawled_date, has_error = self.wait_write_q.get(timeout=20)

        #         # 更新该记录的 crawled_date, has_error
        #         if flag == 0:
        #             sql = """UPDATE crawl 
        #                 SET `crawled_date` = %s, `has_error` = %s
        #                 WHERE `unique` = %s"""
        #             self.db_semaphore.acquire()
        #             try:
        #                 self.cursor.execute(sql, (crawled_date, has_error, unique))
        #                 self.conn.commit()
        #             except pymysql.DatabaseError:
        #                 self.conn.rollback()
        #                 logger1.error('*****WriteThread: update ({}, {}, {}, {}) error'.format(unique, name, crawled_date, has_error))
        #             self.db_semaphore.release()

        #         # 插入该记录的 unique, name, level
        #         elif flag == 1:
        #             sql = """INSERT IGNORE crawl (`unique`, `name`, `level`)
        #                 VALUES (%s, %s, %s)"""
        #             self.db_semaphore.acquire()
        #             try:
        #                 self.cursor.execute(sql, (unique, name, level))
        #                 self.conn.commit()
        #             except pymysql.DatabaseError:
        #                 self.conn.rollback()
        #                 logger1.error('*****WriteThread: insert ({}, {}, {}) error'.format(unique, name, level))
        #             self.db_semaphore.release()
                
        #     except queue.Empty:
        #         logger1.info('*****WriteThread: No data in wait_write_q')
        #         logger1.info('*****WriteThread end*****')
        #         return

        # 批量写入
        update_list = []
        insert_list = []
        while 1:
            try:
                # 若待写队列20s没有数据
                flag, unique, name, level, crawled_date, has_error = self.wait_write_q.get(timeout=20)

                if flag == 0:
                    update_list.append((crawled_date, has_error, unique))

                    # 若更新列表长度为500，一次性写入数据库
                    if (len(update_list) == 500):
                        sql = """UPDATE crawl 
                            SET `crawled_date` = %s, `has_error` = %s
                            WHERE `unique` = %s"""
                        self.db_semaphore.acquire()
                        try:
                            self.cursor.executemany(sql, update_list)
                            self.conn.commit()
                        except pymysql.DatabaseError:
                            self.conn.rollback()
                            logger1.error('*****WriteThread: update error')
                        self.db_semaphore.release()

                        update_list.clear()

                elif flag == 1:
                    insert_list.append((unique, name, level))

                    # 若更新列表长度为500，一次性写入数据库
                    if (len(insert_list) == 500):
                        sql = """INSERT IGNORE crawl (`unique`, `name`, `level`)
                        VALUES (%s, %s, %s)"""
                        self.db_semaphore.acquire()
                        try:
                            self.cursor.executemany(sql, insert_list)
                            self.conn.commit()
                        except pymysql.DatabaseError:
                            self.conn.rollback()
                            logger1.error('*****WriteThread: insert error')
                        self.db_semaphore.release()

                        insert_list.clear()

            except queue.Empty:
                # 将列表内的数据写入数据库
                sql = """UPDATE crawl 
                    SET `crawled_date` = %s, `has_error` = %s
                    WHERE `unique` = %s"""
                self.db_semaphore.acquire()
                try:
                    self.cursor.executemany(sql, update_list)
                    self.conn.commit()
                except pymysql.DatabaseError:
                    self.conn.rollback()
                    logger1.error('*****WriteThread: update error')

                sql = """INSERT IGNORE crawl (`unique`, `name`, `level`)
                        VALUES (%s, %s, %s)"""
                try:
                    self.cursor.executemany(sql, insert_list)
                    self.conn.commit()
                except pymysql.DatabaseError:
                    self.conn.rollback()
                    logger1.error('*****WriteThread: insert error')
                self.db_semaphore.release()

                logger1.info('*****WriteThread: No data in wait_write_q')
                logger1.info('*****WriteThread end*****')
                return


readThread = ReadThread(cursor, wait_crawl_q, db_semaphore)
crawlThreaad1 = CrawlThread('CrawlThread-1', wait_crawl_q, wait_write_q)
crawlThreaad2 = CrawlThread('CrawlThread-2', wait_crawl_q, wait_write_q)
writeThred = WriteThred(conn, cursor, wait_write_q, db_semaphore)

readThread.start()
crawlThreaad1.start()
crawlThreaad2.start()
writeThred.start()

readThread.join()
crawlThreaad1.join()
crawlThreaad2.join()
writeThred.join()

cursor.close()
conn.close()

for h in logger1.handlers:
    h.close()
    logger1.removeHandler(h)

logger1.info('Single time crawled: {}'.format(single_time_crawled))
logger1.info('=====Main Thread End=====')
