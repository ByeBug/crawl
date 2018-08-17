"""
将带单位的金额转化为数字
"""


import re


def convert_to_number(string):
    number = 0

    # 无数据
    if string == '--':
        return number

    # 逗号分割
    if string.find(',') != -1:
        number = float(string.replace(',', ''))

        return number

    # 12.2亿
    pattern = re.compile(r'(\d+(\.\d+)?)(\D+)?')

    m = pattern.match(string)

    if m:
        number = m.group(1)
        unit = m.group(3)

        if unit:
            if unit.find('亿') != -1:
                number = float(number) * 100000000
            elif unit.find('万') != -1:
                number = float(number) * 10000

        return number


if __name__ == '__main__':
    s = '--'

    n = convert_to_number(s)

    print(n)
