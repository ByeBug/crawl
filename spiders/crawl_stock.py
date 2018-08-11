# coding: utf-8
"""
从东方财富和巨潮获取股票信息
不再从巨潮获取额外B股信息
"""

from spiders.crawl_eastmoney import crawl_from_eastmoney
from spiders.crawl_cninfo import crawl_from_cninfo


def crawl_stock(code):
    eastmoney = crawl_from_eastmoney(code)

    # if e['b_stock_info']:
    #     crawl_from_cninfo(e['b_stock_info']['code'])

    cninfo = crawl_from_cninfo(code)

    return eastmoney, cninfo

if __name__ == '__main__':
    code = '600221'

    eastmoney, cninfo = crawl_stock(code)
