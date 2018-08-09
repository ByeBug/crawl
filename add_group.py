"""
为数据库中的公司添加所属集团
c_client 表中添加 所属集团(group)、所属集团根公司id(group_id)、所属集团占比(group_proportion)
"""

import pymysql


# 根据母公司及其所属集团，判断子公司所属集团
# 若投资比例小于10%，则不算
# 若大于其原来的所属集团占比，则更新
# mu_id: 母公司id, mu_group: 母公司所属集团
def func(conn, cursor, mu_id, mu_group, mu_group_id):
    sql = 'select i_id, convert(rate, decimal(5,2)), name from c_investment where c_id=%s'
    cursor.execute(sql, mu_id)
    companies = cursor.fetchall()
    for c in companies:
        print(c[2])
        rate = c[1]
        if float(rate) == 0:    # 原投资占比为'-'
            sql = 'select group_proportion from c_client where c_id=%s'
            # 可能客户表中没这个客户
            if cursor.execute(sql, c[0]):
                proportion = cursor.fetchone()[0]
                if proportion is None:
                    sql = """update c_client
                    set `group`=%s, group_id=%s, group_proportion=%s
                    where c_id=%s"""
                    cursor.execute(sql, (mu_group, mu_group_id, rate, c[0]))
                    conn.commit()
                    func(conn, cursor, c[0], mu_group, mu_group_id)
        elif float(rate) > 10:  # 原投资占比大于10
            sql = 'select group_proportion from c_client where c_id=%s'
            # 可能客户表中没这个客户
            if cursor.execute(sql, c[0]):
                proportion = cursor.fetchone()[0]
                if proportion:
                    if rate > proportion:
                        sql = """update c_client
                        set `group`=%s, group_id=%s, group_proportion=%s
                        where c_id=%s"""
                        cursor.execute(sql, (mu_group, mu_group_id, rate, c[0]))
                        conn.commit()
                        func(conn, cursor, c[0], mu_group, mu_group_id)
                else:
                    sql = """update c_client
                    set `group`=%s, group_id=%s, group_proportion=%s
                    where c_id=%s"""
                    cursor.execute(sql, (mu_group, mu_group_id, rate, c[0]))
                    conn.commit()
                    func(conn, cursor, c[0], mu_group, mu_group_id)


# 给定集团和其根企业列表
groups = [
    {
        'group': '瓮福集团',
        'company': '瓮福(集团)有限责任公司'
    }
]

conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='root', db='financing_test4', charset='utf8')
cursor = conn.cursor()

for g in groups:
    sql = 'select c_id from c_client where name=%s'
    cursor.execute(sql, g['company'])
    g_id = cursor.fetchone()[0]
    # 更新根公司
    sql = """update c_client
    set `group`=%s, group_id=%s, group_proportion=%s
    where c_id=%s"""
    cursor.execute(sql, (g['group'], g_id, 100, g_id))
    func(conn, cursor, g_id, g['group'], g_id)

cursor.close()
conn.close()
