# -*- coding: utf-8 -*-
"""
Created on Sat May 26 02:25:41 2018

@author: storm

logging模块测试文件
"""

import logging

logger1 = logging.getLogger('logger1')
logger1.setLevel(logging.INFO)
handler1 = logging.FileHandler('logging_test_1.log', mode='w', encoding='utf-8')
logger1.addHandler(handler1)

logger2 = logging.getLogger('logger2')
logger2.setLevel(logging.INFO)
handler2 = logging.FileHandler('logging_test_2.log', mode='w', encoding='utf-8')
logger2.addHandler(handler2)

logger1.debug('debug message 1')
logger1.info('info message 1')
logger1.warning('warning message 1')
logger1.error('error message 1')
logger1.critical('critical message 1')

logger2.debug('debug message 2')
logger2.info('info message 2')
logger2.warning('warning message 2')
logger2.error('error message 2')
logger2.critical('critical message 2')

try:
    3/0
except Exception as e:
    logger1.exception(e)
    logger1.error('has error')


handler1.close()
handler2.close()
logger1.removeHandler(handler1)
logger2.removeHandler(handler2)