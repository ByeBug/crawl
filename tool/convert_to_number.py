"""
将带单位的金额转化为数字
"""


import re


def convert_to_number(string):
    pattern = re.compile(r'(\d+(\.\d+)?)(\D+)?')

    m = pattern.match(string)

    number = m.group(1)
    unit = m.group(3)

    if unit:
        if unit.find('亿') != -1:
            number = float(number) * 100000000
        elif unit.find('万') != -1:
            number = float(number) * 10000

    return number


if __name__ == '__main__':
    s = '12.2'

    n = convert_to_number(s)

    print(n)