"""
为数据库中的公司添加所属集团
c_client 表中添加 所属集团(group)、所属集团根公司id(group_id)、所属集团占比(group_proportion)
"""
import os
import configparser

import pymysql


if not os.path.isfile('myconfig.cfg'):
    print("myconfig.cfg doesn't exist")
    exit()

config = configparser.RawConfigParser()
config.read('myconfig.cfg', encoding='utf-8')

try:
    store_db_host = config['store_db']['host']
    store_db_port = config['store_db']['port']
    store_db_user = config['store_db']['user']
    store_db_passwd = config['store_db']['passwd']
    store_db_db = config['store_db']['db']
    conn = pymysql.connect(host=store_db_host, port=int(store_db_port), 
                            user=store_db_user, passwd=store_db_passwd, 
                            db=store_db_db, charset='utf8')
    cursor = conn.cursor()
except Exception as e:
    print('Connect to store_db failed')
    print(e)
    exit()


# 根据母公司及其所属集团，判断子公司所属集团
# 若投资比例小于10%，则不算
# 若大于其原来的所属集团占比，则更新
# mu_id: 母公司id, mu_group: 母公司所属集团
def func(conn, cursor, mu_id, mu_group, mu_group_id):
    sql = 'SELECT `i_id`, CONVERT(`rate`, DECIMAL(5,2)), `name` FROM c_investment WHERE c_id=%s'
    cursor.execute(sql, mu_id)
    companies = cursor.fetchall()
    for c in companies:
        rate = c[1]
        if float(rate) == 0:    # 原投资占比为'-'
            sql = 'SELECT `group_id`, `group_proportion` FROM c_client WHERE c_id=%s'
            # 可能客户表中没这个客户
            if cursor.execute(sql, c[0]):
                group_id, proportion = cursor.fetchone()
                if group_id is None:
                    sql = """UPDATE c_client
                    SET `group`=%s, `group_id`=%s, `group_proportion`=%s
                    WHERE c_id=%s"""
                    cursor.execute(sql, (mu_group, mu_group_id, rate, c[0]))
                    conn.commit()
                    func(conn, cursor, c[0], mu_group, mu_group_id)
        elif float(rate) > 10:  # 原投资占比大于10
            sql = 'SELECT `group_id`, `group_proportion` FROM c_client WHERE c_id=%s'
            # 可能客户表中没这个客户
            if cursor.execute(sql, c[0]):
                group_id, proportion = cursor.fetchone()
                if group_id:
                    if group_id == mu_group_id:
                        if rate > proportion:
                            sql = """UPDATE c_client
                            SET `group`=%s, `group_id`=%s, `group_proportion`=%s
                            WHERE c_id=%s"""
                            cursor.execute(sql, (mu_group, mu_group_id, rate, c[0]))
                            conn.commit()
                    else:
                        if rate > proportion:
                            sql = """UPDATE c_client
                            SET `group`=%s, `group_id`=%s, `group_proportion`=%s
                            WHERE c_id=%s"""
                            cursor.execute(sql, (mu_group, mu_group_id, rate, c[0]))
                            conn.commit()
                            func(conn, cursor, c[0], mu_group, mu_group_id)
                else:
                    sql = """UPDATE c_client
                    SET `group`=%s, `group_id`=%s, `group_proportion`=%s
                    WHERE c_id=%s"""
                    cursor.execute(sql, (mu_group, mu_group_id, rate, c[0]))
                    conn.commit()
                    func(conn, cursor, c[0], mu_group, mu_group_id)


# 给定集团和其根企业列表
sql = 'SELECT `unique`, `root_name` FROM groups'
cursor.execute(sql)
groups = cursor.fetchall()
groups_num = len(groups)

for i, g in enumerate(groups, 1):
    g_id, g_name = g
    print('{}/{} -- {}'.format(i, groups_num, g_name))
    # 更新根公司
    sql = """UPDATE c_client
    SET `group`=%s, `group_id`=%s, `group_proportion`=%s
    WHERE c_id=%s"""
    cursor.execute(sql, (g_name, g_id, 100, g_id))
    func(conn, cursor, g_id, g_name, g_id)

cursor.close()
conn.close()
