# -*- coding: utf-8 -*-
"""
Created on Sun May 27 22:43:13 2018

@author: storm

测试企查查使用代理的效果
"""

import requests
from crawl_qichacha import NeedValidationError
from bs4 import BeautifulSoup as bs

proxy = '221.7.255.168:8080'
proxy_dict = {'http': proxy, 'https': proxy}


url = 'https://httpbin.org/get'
url = 'https://www.baidu.com'
url = 'https://www.qichacha.com/firm_24bb12e95b13cfa0c2394214e2b60f50.html'


headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'accept-encoding': 'gzip, deflate, sdch, br',
        'accept-language': 'zh-CN,zh;q=0.8',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
        'cookie': 'UM_distinctid=1631b41c6ec83a-06a32db82facd3-5d4e211f-1fa400-1631b41c6ed730; _uab_collina=152517069787150778662496; acw_tc=AQAAAFLidRmzTQ4AGlxH0yhTxSVeGQgj; PHPSESSID=piemukq85l1k7mi3hri1f5ki72; _umdata=0712F33290AB8A6DAA9492599E5221BD3237218696EEFAD2ABC21DB1ED3694ECD448655B4B2FC5B6CD43AD3E795C914C48D38E51BA04D1DA5F911FCCBA849FA1; hasShow=1; zg_did=%7B%22did%22%3A%20%221631b41c70b27b-05b6a9a9370726-5d4e211f-1fa400-1631b41c70c301%22%7D; CNZZDATA1254842228=289668161-1525166267-%7C1527492997; zg_de1d1a35bfa24ce29bbf2c7eb17e6c4f=%7B%22sid%22%3A%201527491497143%2C%22updated%22%3A%201527493340458%2C%22info%22%3A%201527078899024%2C%22superProperty%22%3A%20%22%7B%7D%22%2C%22platform%22%3A%20%22%7B%7D%22%2C%22utm%22%3A%20%22%7B%7D%22%2C%22referrerDomain%22%3A%20%22%22%2C%22cuid%22%3A%20%22891c207602271c765c43d2bf8d2e4419%22%7D; Hm_lvt_3456bee468c83cc63fb5147f119f1075=1527305942,1527307570,1527333310,1527392263; Hm_lpvt_3456bee468c83cc63fb5147f119f1075=1527493341',
}

r = requests.get('http://127.0.0.1:8000/?protocol=0&country=国内', timeout=4)

proxy_list = r.json()
proxy_list.reverse()
print('proxy num:', len(proxy_list))

good = bad = validate = 0

for p in proxy_list:
    proxy = '{}:{}'.format(p[0], p[1])
    proxy_dict = {'http': proxy, 'https': proxy}
    try:
        r = requests.get(url, timeout=5, headers=headers, proxies=proxy_dict)
        r.raise_for_status()
        
        if str(r.text).find('http://www.qichacha.com/index_verify') != -1:
            raise NeedValidationError('Need Browser validate: ' + r.url)
        
        s = bs(r.text, 'html.parser')
        
        if s.script.string:
            if s.script.string.find('var arg1=') != -1:
                raise NeedValidationError('Need Browser Open url: ' + r.url)
                
    except requests.RequestException:
        bad += 1
        print(proxy, 'is bad')
    except NeedValidationError:
        validate += 1
        print(proxy, 'need validate')
    else:
        good += 1
        print(proxy, 'is good')
print('total: {}, good: {}, bad: {}, validate: {}'.format(len(proxy_list), good, bad, validate))

