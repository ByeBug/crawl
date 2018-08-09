# coding: utf-8
"""
从东方财富和巨潮获取股票信息，
分别保存为json文件
"""

from spiders.crawl_eastmoney import crawl_from_eastmoney
from spiders.crawl_cninfo import crawl_from_cninfo


def crawl_stock(code):
    e = crawl_from_eastmoney(code)

    if e['b_stock_info']:
        crawl_from_cninfo(e['b_stock_info']['code'])

    crawl_from_cninfo(code)


if __name__ == '__main__':
    code = '600221'

    crawl_stock(code)
