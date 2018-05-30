# coding: utf-8
"""
信号量 测试模型
"""
import threading
import logging

logging.basicConfig(level=logging.INFO)
s = threading.Semaphore(1)


def fun1(s):
    for i in range(3):
        logging.info('-------time: ' + str(i+1))
        logging.info('11111 fun1 acquiring semaphore')
        s.acquire()
        logging.info('11111 fun1 acquired s, doing something')
        s.release()
        logging.info('11111 fun1 release s')
        logging.info('-------time: ' + str(i+1) + ' end-----\n')


def fun2(s):
    for i in range(10):
        logging.info('+++++++time: ' + str(i+1))
        logging.info('22222 fun2 acquiring semaphore')
        s.acquire()
        logging.info('22222 fun2 acquired s, doing something')
        s.release()
        logging.info('22222 fun2 release s')
        logging.info('+++++++time: ' + str(i+1) + ' end+++++++\n')


th1 = threading.Thread(target=fun1, args=(s, ))
th2 = threading.Thread(target=fun2, args=(s, ))

th1.start()
th2.start()

th1.join()
th2.join()

logging.info('main end')
