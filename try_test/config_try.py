"""
try configparser
"""

import configparser

# cookie_str 中带百分号，只能使用 RawConfigParser
config = configparser.RawConfigParser()
config.read('../config.cfg', encoding='utf-8')

print(config['crawl']['save_path'])