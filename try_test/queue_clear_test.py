# coding: utf-8
"""
清空队列 测试模型
"""
import threading, queue, logging

q = queue.Queue()

def consumer(name):
    while not q.empty():
        i = q.get_nowait()
        logging.warning(name + ' ' + str(i) + ' ' + str(q.qsize()))
        q.task_done()
    logging.warning(name + ' clear all')

def productor():
    for i in range(10):
        q.put(i)
    
    q.join()
    logging.warning('q wake up')

p_th = threading.Thread(name='p_th', target=productor)

c_th1 = threading.Thread(name='c_th1', target=consumer, args=('c_th1', ))
c_th2 = threading.Thread(name='c_th2', target=consumer, args=('c_th2', ))

p_th.start()

c_th1.start()
c_th2.start()

p_th.join()

c_th1.join()
c_th2.join()

logging.warning('main end')
