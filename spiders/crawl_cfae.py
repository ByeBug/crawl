# -*- coding: utf-8 -*-
"""
Created on Fri Jul  6 15:07:28 2018

@author: storm
爬取北金所的pdf列表
"""

import time, json

import requests


"""
title -- 信息披露标题
menuId -- 菜单
  债务融资工具(DCM) -- 17
    发行披露 -- 56
      发行文件 -- 121
      申购说明 -- 122
      招标文件 -- 181
      其他与发行相关的事项 -- 123
    发行结果 -- 57
      发行情况公告 -- 124
      招标情况公告 -- 182
      交易流通要素公告 -- 125
    信用评级 -- 58
      主体评级 -- 126
      债项披露 --127
    财务报告 -- 59
      一季度财务报告 -- 128
      半年度财务报告 -- 129
      三季度财务报告 -- 130
      年度报告及审计报告 -- 131
      财务报告延迟披露说明 -- 132
    ABN定期报告 -- 60
      受托机构报告 -- 133
    付息兑付 -- 61
      付息兑付公告 -- 134
      风险提示相关公告 -- 135
      增进义务履行情况相关公告 -- 136
    重大事项及其他 -- 62
      重大事项（机构） -- 137
      重大事项（债项） -- 281
      中介机构专项意见 -- 138
    持有人会议 -- 63
      持有人会议 -- 139
    其他 -- 64
      其他 -- 140
  信贷资产支持证券(ABS) -- 18
    注册阶段 -- 66
      注册申请报告 -- 141
    发行阶段 -- 67
      发行文件 -- 142
      申购文件 -- 143
      发行情况公告 -- 144
      成立公告 -- 145
      交易流通要素公告 -- 146
    存续与兑付阶段 -- 68
      受托机构报告 -- 147
      跟踪评级报告 -- 148
      重大事项公告 -- 149
      持有人大会公告 -- 150
      付息兑付公告 -- 151
    其他 -- 69
      其他 -- 152
bondType -- 债券类型
  全部 -- ''
  超短期融资券 -- 105
  短期融资券 -- 101
  中期票据 -- 102
  资产支持票据 -- 106
  绿色债务融资工具 -- 113
  中小企业集合票据 -- 103
  区域集优中小企业集合票据 -- 104
timeStart -- 信息披露时间起 -- 2017-07-06
timeEnd -- 信息披露时间止 -- 2018-07-06
bondShortName -- 债券简称
bondCode -- 债券代码
publishOrg -- 发行人
leadManager -- 主承销商 -- 承销商ID
  先通过 leadManagerQuery 查询主承销商获取ID
"""

def crawl_cfae(title, menuID=17):
    announcements_url = 'https://www.cfae.cn/connector/selectAllInfoNew'
    files_url = 'https://www.cfae.cn/connector/selectFileInfoById'

    data = {
        'title': title,
        'pageNumber': 1,
        'menuId': menuID,
        'bondType' ''
        'timeStart': '',
        'timeEnd': '',
        'bondShortName': '',
        'bondCode': '',
        'publishOrg': '',
        'leadManager': ''
    }

    headers = {
        'Host': 'www.cfae.cn',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'
    }

    announcements = []

    # 公告列表
    while 1:
        r = requests.post(announcements_url, data=data, timeout=4, headers=headers)
        r.raise_for_status()

        s = r.json()
        
        print('crawled {}/{} page'.format(s['pageNo'], s['totalPage']))

        for l in s['list']:
            announcement = l
            announcement['disTime'] = time.strftime('%Y-%m-%d', time.localtime(int(announcement['disTime']/1000)))
            announcement['publish_time_str'] = time.strftime('%Y-%m-%d', time.localtime(int(announcement['publish_time_str']/1000)))
            announcement['files'] = []

            # 该公告对应的文件
            temp_r = requests.post(files_url, data={'infoId': announcement['info_id']}, timeout=4, headers=headers)
            temp_r.raise_for_status()

            temp_s = temp_r.json()
            for item in temp_s:
                f = {
                    'file_address': item['FILE_ADDRESS'],
                    'file_name': item['FILE_NAME'],
                    'file_download_url': 'https://www.cfae.cn/SFTP/download?fileName={}&fileAdd={}'.format(item['FILE_NAME'], item['FILE_ADDRESS'])
                }
                announcement['files'].append(f)

            announcements.append(announcement)

        if s['lastPage']:
            break
        
        data['pageNumber'] += 1

    return announcements

# # 保存为文件
# json_name = 'g:/cfae_announcements_' + time.strftime('%Y-%m-%d', time.localtime(time.time())) + '.json'
# with open(json_name, 'w', encoding='utf-8') as f:
#     json.dump(announcements, f, ensure_ascii=False)

if __name__ == '__main__':
    title = '海航集团'
    menuID = 17

    announcements = crawl_cfae(title, menuID)
