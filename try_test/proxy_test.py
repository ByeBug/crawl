"""
@author: storm

多线程使用代理的模型
"""


import requests, logging, threading, time, json

logging.basicConfig(level=logging.INFO)

semaphore_proxy = threading.Semaphore(1)
event_proxy_less_than_10 = threading.Event()

proxy_dict = {}
proxy_del = []

program_over = False

test_num = 0

with open('config.json', 'r') as f:
    config = json.load(f)
    cookie_str = config['cookie_qichacha']
    user_agent = config['user_agent']

headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'accept-encoding': 'gzip, deflate, sdch, br',
    'accept-language': 'zh-CN,zh;q=0.8',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'upgrade-insecure-requests': '1',
    'user-agent': user_agent
}
cookies = {}
for cookie in cookie_str.split(';'):
    k, v = cookie.strip().split('=')
    cookies[k] = v

def get_proxy(name):
    while 1:
        r = requests.get('http://127.0.0.1:8000/?protocol=0&country=国内', timeout=4)

        proxy_list = r.json()
        # 先使用分数低的代理
        proxy_list.reverse()
        logging.info(' {} -- proxy num: {}'.format(name, len(proxy_list)))

        semaphore_proxy.acquire()
        for p in proxy_list:
            key = '{}:{}'.format(p[0], p[1])
            proxy_dict[key] = p
        semaphore_proxy.release()

        event_proxy_less_than_10.clear()

        event_proxy_less_than_10.wait()
        logging.info(' {} wake up'.format(name))
        
        semaphore_proxy.acquire()
        logging.info(' {}: deleting {} proxies'.format(name, len(proxy_del)))
        for p in proxy_del:
            url = 'http://127.0.0.1:8000/delete?ip={}&port={}'.format(p[0], p[1])
            requests.get(url, timeout=4)
            logging.info(' {}: delete {}:{}'.format(name, p[0], p[1]))
        proxy_del.clear()
        semaphore_proxy.release()
        
        if program_over:
            logging.info(' {}: Program Over, Quit'.format(name))
            return


def use_proxy(name):
    global test_num, program_over
    
    time.sleep(2)
    while test_num < 200:
        semaphore_proxy.acquire()
        proxy = list(proxy_dict.keys())[0]
        score = proxy_dict[proxy][2]

        test_num += 1

        semaphore_proxy.release()

        proxies = {'https': proxy}

        url = 'https://www.qichacha.com'

        logging.info(' {}: testing {} time'.format(name, test_num))

        try:
            requests.get(url, timeout=4, cookies=cookies, headers=headers, proxies=proxies)
        except requests.exceptions.RequestException:
            logging.info(' {}: ({}, {}) is bad'.format(name, proxy, score))

            semaphore_proxy.acquire()
            if proxy in proxy_dict:
                proxy_dict[proxy][2] -= 1
                if proxy_dict[proxy][2] == 0:
                    proxy_del.append(proxy.split(':'))
                    proxy_dict.pop(proxy)
                    logging.info(' {}: Append ({}, {}) to delete'.format(name, proxy, score))
                    if len(proxy_dict) < 10:
                        event_proxy_less_than_10.set()
            semaphore_proxy.release()
        else:
            logging.info(' {}: ({}, {}) is goooood'.format(name, proxy, score))

    logging.info(' {}: test {} times, quit'.format(name, test_num))
    event_proxy_less_than_10.set()
    program_over = True
    return


g_p1 = threading.Thread(target=get_proxy, args=('Get-1', ))

u_p1 = threading.Thread(target=use_proxy, args=('Use-1', ))
u_p2 = threading.Thread(target=use_proxy, args=('Use-2', ))

g_p1.start()

u_p1.start()
u_p2.start()

g_p1.join()

u_p1.join()
u_p2.join()


logging.info('-----main end-----')