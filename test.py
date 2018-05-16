#-*-coding:utf-8-*-

import re
from crawl_qichacha import crawl_from_qichacha

def crawl_qichacha(name, url, crawled):
    qichacha = crawl_from_qichacha(name, url)

    crawled.append(name)

    excepted = ['TANG DYNSTY DEVELOPMENT COMPANY LIMITED', '海南省慈航公益基金会']
    excepted = ['海南省慈航公益基金会']

    print('crawling holders', name)
    for holder in qichacha['holders']:
        if re.search('pl_\w+.html', holder['url']):
            continue
        if holder['name'] in crawled:
            continue
        if holder['name'] in excepted:
            continue
        crawl_qichacha(holder['name'], holder['url'], crawled)
    print('crawl holders finished', name)

    print('crawling investments', name)
    for investment in qichacha['investments']:
        if investment['name'] in crawled:
            continue
        crawl_qichacha(investment['name'], investment['url'], crawled)
    print('crawl investments finished', name)


crawled = []
name = '海航集团有限公司'

crawl_qichacha(name, '', crawled)
