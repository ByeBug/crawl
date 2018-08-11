"""
将日期转换为 yyyy-mm-dd 格式
不合法返回空字符串
"""


import re
import datetime


def convert_to_date(string):
    if re.match(r'\d{2}-\d{2}-\d{2}', string):
        d = datetime.datetime.strptime(string, '%y-%m-%d').date().isoformat()
    else:
        d = ''
    
    return d


if __name__ == '__main__':
    s = '96-08-08'
    
    d = convert_to_date(s)

    print(d)