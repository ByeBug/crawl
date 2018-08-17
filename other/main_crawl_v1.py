# -*- coding: utf-8 -*-
"""
Created on Tue May 22 17:56:34 2018

@author: storm

多线程爬取主程序
"""

import logging
import queue
import random
import re
import sqlite3
import threading
import time

from spiders.crawl_qichacha import NeedValidationError, crawl_from_qichacha
from spiders.crawl_cninfo import crawl_from_cninfo
from spiders.crawl_eastmoney import crawl_from_eastmoney


logger1 = logging.getLogger('logger1')
logger1.setLevel(logging.INFO)
handler1 = logging.FileHandler('crawl_log.log', mode='w', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(message)s', '%H:%M:%S')
handler1.setFormatter(formatter)
stream_handler = logging.StreamHandler()
logger1.addHandler(handler1)
logger1.addHandler(stream_handler)

conn = sqlite3.connect('d:/firm_unique_qichacha.db', check_same_thread=False)
cursor = conn.cursor()

if not cursor.execute("select * from sqlite_master where type='table' and name='firm_unique';").fetchone():
    cursor.execute("""create table firm_unique(
        firm_name text,
        firm_unique text primary key,
        need_crawl text,
        has_error text);""")
    conn.commit()

company_list = ['海航集团有限公司']

wait_crawl_q = queue.Queue()
wait_write_q = queue.Queue()

db_semaphore = threading.Semaphore(1)

need_validate = False
single_time_crawled = 0


def crawl_stock(code):
    try:
        e = crawl_from_eastmoney(code)

        if e['b_stock_info']:
            crawl_from_cninfo(e['b_stock_info']['code'])

        crawl_from_cninfo(code)
    except Exception as e:
        logger1.exception(e)
        logger1.error('crawl stock {} error'.format(code))



class ReadThread(threading.Thread):
    def __init__(self, cursor, company_list, wait_crawl_q, db_semaphore):
        super().__init__()
        self.cursor = cursor
        self.company_list = company_list
        self.wait_crawl_q = wait_crawl_q
        self.db_semaphore = db_semaphore

    def run(self):
        # 公司列表存入队列
        for item in self.company_list:
            sql = "select * from firm_unique where firm_name='%s' and need_crawl=0" % item
            self.cursor.execute(sql)
            if self.cursor.fetchone():
                continue

            name = item
            unique = ''

            self.wait_crawl_q.put((name, unique))
            logger1.info('-----ReadThread put (%s) from list to wait_crawl_q' % name)

        self.wait_crawl_q.join()

        while 1:
            time.sleep(2)
            sql = 'select * from firm_unique where need_crawl=1 and has_error=0'
            logger1.info('-----ReadThread acquiring db')
            self.db_semaphore.acquire()
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            self.db_semaphore.release()
            logger1.info('-----ReadThread release db')
            if result:
                logger1.info('-----ReadThread get {} rows from database'.format(len(result)))
                for row in result:
                    name = row[0]
                    unique = row[1]

                    self.wait_crawl_q.put((name, unique))

                logger1.info('----- ReadThread blocking')
                self.wait_crawl_q.join()
                logger1.info('----- ReadThread Wake up')
                if need_validate:
                    logger1.info('-----ReadThread: Need validate')
                    logger1.info('-----ReadThread end-----')
                    return
            else:
                # read_thread_end = True
                logger1.info('-----ReadThread: No data in database')
                logger1.info('-----ReadThread end-----')
                return


class CrawlThread(threading.Thread):
    def __init__(self, name, wait_crawl_q, wait_write_q):
        super().__init__()
        self.name = name
        self.wait_crawl_q = wait_crawl_q
        self.wait_write_q = wait_write_q

    def run(self):
        global single_time_crawled, need_validate

        while 1:
            try:
                # 等待15s
                # 若待爬队列15s无数据
                name, unique = self.wait_crawl_q.get(timeout=15)
                # logger.info('+++++{} get ({}, {}) from wait_crawl_q'.format(self.name, name, unique))
                if unique:
                    url = 'https://www.qichacha.com/firm_' + unique + '.html'
                else:
                    url = ''

                # 暂时不使用代理
                proxy = None

                # 加入延时
                time.sleep(random.uniform(2, 3))

                try:
                    qichacha = crawl_from_qichacha(name, url, proxy)
                except NeedValidationError as e:
                    wait_write_q_item = (name, unique, 1, 0)
                    self.wait_write_q.put(wait_write_q_item)
                    logger1.info('+++++{} put ({}, {}, {}, {}) into wait_write_q, remain: {}'.format(self.name, *wait_write_q_item, self.wait_crawl_q.qsize()))

                    self.wait_crawl_q.task_done()

                    need_validate = True

                    logger1.error('===!!{} get Need Validation Error, clearing q, qsize: {}, unfinished: {}'.format(self.name, self.wait_crawl_q.qsize(), self.wait_crawl_q.unfinished_tasks))
                    while not self.wait_crawl_q.empty():
                        try:
                            self.wait_crawl_q.get_nowait()
                            self.wait_crawl_q.task_done()
                        except queue.Empty:
                            logger1.error('!!!!!{} get Empty Error when clear q'.format(self.name))
                    logger1.error('===!!{} clear q finished, qsize: {}, unfinished: {}'.format(self.name, self.wait_crawl_q.qsize(), self.wait_crawl_q.unfinished_tasks))

                    continue

                except Exception as e:
                    logger1.exception(e)
                    logger1.error('!!!!!{} crawl ({}, {}) error!!!!!'.format(self.name, name, url))

                    wait_write_q_item = (name, unique, 0, 1)
                    self.wait_write_q.put(wait_write_q_item)
                    logger1.info('+++++{} put ({}, {}, {}, {}) into wait_write_q, remain: {}'.format(self.name, *wait_write_q_item, self.wait_crawl_q.qsize()))

                    self.wait_crawl_q.task_done()

                    single_time_crawled += 1

                    continue

                else:
                    if not unique:
                        url = qichacha['url']
                        unique = re.search(r'firm_(\w+).html', url).group(1)

                    logger1.info('+++++{} crawled ({})'.format(self.name, name))

                    wait_write_q_item = (name, unique, 0, 0)
                    self.wait_write_q.put(wait_write_q_item)
                    logger1.info('+++++{} put ({}, {}, {}, {}) into wait_write_q, remain: {}'.format(self.name, *wait_write_q_item, self.wait_crawl_q.qsize()))

                    if qichacha['overview']['stock_code']:
                        crawl_stock_thread = threading.Thread(target=crawl_stock, args=(qichacha['overview']['stock_code'], ), name='crawl-stock-thread')
                        crawl_stock_thread.start()

                    for holder in qichacha['holders']:
                        unique = re.search(r'firm_(\w+).html', holder['url'])
                        if not unique:
                            continue
                        unique = unique.group(1)
                        name = holder['name']
                        wait_write_q_item = (name, unique, 1, 0)
                        self.wait_write_q.put(wait_write_q_item)

                    for investment in qichacha['investments']:
                        unique = re.search(r'firm_(\w+).html', investment['url'])
                        if not unique:
                            continue
                        unique = unique.group(1)
                        name = investment['company_name']
                        wait_write_q_item = (name, unique, 1, 0)
                        self.wait_write_q.put(wait_write_q_item)
                        
                    self.wait_crawl_q.task_done()

                    single_time_crawled += 1

            except queue.Empty:
                logger1.info('+++++{}: No data in wait_crawl_q'.format(self.name))
                logger1.info('+++++{} end+++++'.format(self.name))

                return


class WriteThred(threading.Thread):
    def __init__(self, conn, cursor, wait_write_q, db_semaphore):
        super().__init__()
        self.conn = conn
        self.cursor = cursor
        self.wait_write_q = wait_write_q
        self.db_semaphore = db_semaphore

    def run(self):
        while 1:
            try:
                # 等待20秒
                # 若待写队列20s没有数据
                name, unique, need_crawl, has_error = self.wait_write_q.get(timeout=20)
                sql = ''
                try:
                    self.db_semaphore.acquire()
                    sql = "insert into firm_unique values ('{}', '{}', {}, {})".format(name, unique, need_crawl, has_error)
                    self.cursor.execute(sql)
                    self.conn.commit()
                except sqlite3.Error:
                    self.conn.rollback()
                    if not need_crawl:
                        sql = "update firm_unique set need_crawl=0, has_error={} where firm_unique='{}' and need_crawl=1".format(has_error, unique)
                        self.cursor.execute(sql)
                        self.conn.commit()
                finally:
                    self.db_semaphore.release()

            except queue.Empty:
                logger1.info('*****WriteThread: No data in wait_write_q')
                logger1.info('*****WriteThread end*****')
                return


readThread = ReadThread(cursor, company_list, wait_crawl_q, db_semaphore)
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
