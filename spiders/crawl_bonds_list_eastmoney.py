# -*- coding: utf-8 -*-
"""
Created on Sat May 19 22:33:00 2018

@author: storm

从东方财富爬取债券列表
沪企债，沪转债，深企债，深转债
"""

import re
import time
import sqlite3
import requests


def crawl_bonds_list_eastmoney():
    conn = sqlite3.connect('test_bond_list.db')
    cursor = conn.cursor()
    try:
        sql = "select * from sqlite_master where type='table' and name='bonds_list';"
        if not cursor.execute(sql).fetchone():
            cursor.execute("""create table bonds_list(code text primary key,
                            name text, type text, market text, update_date text);""")
            conn.commit()
    except Exception as e:
        conn.rollback()
        print('error:', e)

    url = 'http://nufm.dfcfw.com/EM_Finance2014NumericApplication/JS.aspx'

    headers = {
        'Host': 'nufm.dfcfw.com',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'
    }
    payload = {
        'type': 'CT',
        'cmd': '',
        'sty': 'FCOIATA',
        'sortType': '(Code)',
        'sortRule': 1,
        'page': '',
        'pageSize': 500,
        'js': '{rank:[(x)],pages:(pc),total:(tot)}',
        'token': '7bc05d0d4c3c22ef9fca8c2a912d779c',
    }

    dicts = [{'type': 'HQ', 'cmd': 'C._DEBT_SH_Q', 'market': 'SH'},
             {'type': 'HZ', 'cmd': 'C._DEBT_SZ_Q', 'market': 'SH'},
             {'type': 'SQ', 'cmd': 'C._DEBT_SH_Z', 'market': 'SZ'},
             {'type': 'SZ', 'cmd': 'C._DEBT_SZ_Z', 'market': 'SZ'}]

    update_date = time.strftime('%Y-%m-%d', time.localtime())

    counter = 0

    for d in dicts:
        payload['page'] = 1
        payload['cmd'] = d['cmd']

        r = requests.get(url, timeout=4, headers=headers, params=payload)
        r.raise_for_status()

        print('#### Get {} page {}, start writing to db'.format(
            d['type'], payload['page']))

        pages = int(re.search(r'pages:(\d+)', r.text).group(1))

        items = re.findall('"([^"]*)"', r.text)
        for i in items:
            l = i.split(',')
            code = l[1]
            name = l[2]

            try:
                sql = """insert into bonds_list
                        values ('{}', '{}', '{}', '{}', '{}');""".format(code, name, d['type'], d['market'], update_date)
                cursor.execute(sql)
                conn.commit()
            except sqlite3.DatabaseError:
                sql = """update bonds_list 
                        set name='{}', type='{}', market='{}', update_date='{}'
                        where code='{}';""".format(name, d['type'], d['market'], update_date, code)
                cursor.execute(sql)
                conn.commit()

            counter += 1
            print('write item', counter)

        print('#### end write', counter)

        page = 2
        while page <= pages:
            payload['page'] = page
            r = requests.get(url, timeout=4, headers=headers, params=payload)
            r.raise_for_status()

            print('#### Get {} page {}, start writing to db'.format(
                d['type'], payload['page']))

            items = re.findall('"([^"]*)"', r.text)
            for i in items:
                l = i.split(',')
                code = l[1]
                name = l[2]

                try:
                    sql = """insert into bonds_list
                            values ('{}', '{}', '{}', '{}', '{}');""".format(code, name, d['type'], d['market'], update_date)
                    cursor.execute(sql)
                    conn.commit()
                except sqlite3.DatabaseError:
                    sql = """update bonds_list 
                            set name='{}', type='{}', market='{}', update_date='{}'
                            where code='{}';""".format(name, d['type'], d['market'], update_date, code)
                    cursor.execute(sql)
                    conn.commit()

                counter += 1
                print('write item', counter)

            print('#### end write', counter)

            page += 1

    cursor.close()
    conn.close()


if __name__ == '__main__':
    crawl_bonds_list_eastmoney()
