# -*- coding: utf-8 -*-
"""
Created on Fri Jun 22 12:31:52 2018

@author: storm
爬取巨潮的公告，将pdf下载到本地
"""

import re, os, urllib, time
import requests


def get_file_list(stock):
    file_list = []

    filelist_url = 'http://www.cninfo.com.cn/cninfo-new/disclosure/szse/fulltext'

    headers = {
        'Host': 'www.cninfo.com.cn',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'
    }

    payload = {
        'stock': stock,
        'searchkey': '',
        'category': '',
        'pageNum': 1,
        'pageSize': 50,
        'column': 'sse',
        'tabName': 'latest',
        'sortName': '',
        'sortType': '',
        'limit': '',
        'seDate': ''
    }

    while 1:
        r = requests.post(filelist_url, data=payload, timeout=4, headers=headers)
        r.raise_for_status()

        s = r.json()

        for classified_announcement in s['classifiedAnnouncements']:
            for announcement in classified_announcement:
                item = {
                    'adjunctSize': announcement['adjunctSize'],
                    'adjunctType': announcement['adjunctType'],
                    'adjunctUrl': announcement['adjunctUrl'],
                    'announcementTime': time.strftime('%Y-%m-%d', time.localtime(announcement['announcementTime'] / 1000)),
                    'announcementTitle': announcement['announcementTitle'],
                    'announcementTypeName': announcement['announcementTypeName']
                }
                file_list.append(item)
        
        if not s['hasMore']:
            break

        payload['pageNum'] += 1

    print('{} files'.format(len(file_list)))

    return file_list


def download_files(file_list):
    path = 'pdfs/'
    if not os.path.isdir(path):
        os.makedirs(path)

    nums = len(file_list)

    headers = {
        'Host': 'www.cninfo.com.cn',
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0'
    }

    for i, file_info in enumerate(file_list):
        url = 'http://www.cninfo.com.cn/' + file_info['adjunctUrl']
        
        filename = ''.join([path, file_info['announcementTitle'], '@', file_info['announcementTime'], '.pdf'])

        if os.path.isfile(filename):
            print('skip', filename)
            continue

        r = requests.get(url, timeout=4, stream=True, headers=headers)
        r.raise_for_status()

        print('downloading {}/{}, {}k, {}'.format(i+1, nums, file_info['adjunctSize'], filename))
        
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(10485760):  # 缓存为10m
                f.write(chunk)
        

if __name__ == '__main__':
    stock = '600221'

    file_list = get_file_list(stock)

    download_files(file_list)








# url = 'http://www.cninfo.com.cn/cninfo-new/disclosure/sse/download/1203487186?announceTime=2017-05-10'

# r = requests.get(url, timeout=4, stream=True)
# r.raise_for_status()

# filename = re.search(r'\"(.*)\"', r.headers['content-disposition']).group(1)
# filename = urllib.parse.unquote(filename)

# with open(filename, 'wb') as f:
#     for chunk in r.iter_content(4096):
#         f.write(chunk)

# print('finish')