"""
更改变更信息
将“；【新增】”
改为“【新增】；”
"""


import re


def change_changeinfo(string):
    pattern = re.compile(r'[;；]【\w+】')

    s1 = list(set(pattern.findall(string)))
    s2 = [i[1:]+i[0] for i in s1]

    for i in range(len(s1)):
        string = string.replace(s1[i], s2[i])

    return string


if __name__ == '__main__':
    s = '黎静；【新增】张三;【退出】'

    n = change_changeinfo(s)

    print(n)