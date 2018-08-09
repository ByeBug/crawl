"""
遍历 BigCustomer.list
在企查查中查询对应的企业
"""


import json, time

from spiders.crawl_qichacha import get_config, search_company, NeedValidationError


with open('BigCustomer.list', 'r', encoding='utf-8') as f:
    old_list = [i.strip() for i in f.readlines()]

headers, cookies = get_config('g:/crawl')

with open('Big_Customer_result.json', 'r', encoding='utf-8') as f:
    result = json.load(f)

searched = [i['o_name'] for i in result]
total = len(old_list)

# for i in result:
#     if i['n_name'] == '':
#         print('searching %s' % i['o_name'])
#         try:
#             n, u = search_company(i['o_name'], cookies, headers)
#             i['n_name'] = n
#             i['url'] = u
#             print('new name %s' % i['n_name'])
#         except Exception as e:
#             with open('Big_Customer_result.json', 'w', encoding='utf-8') as f:
#                 json.dump(result, f, ensure_ascii=False)
#             raise e

for i, name in enumerate(old_list, 1):
    if name in searched:
        print('skip %d/%d %s' % (i, total, name))
        continue
    print('searching %d/%d %s' % (i, total, name))
    try:
        n, u = search_company(name, cookies, headers)
        result.append({
            'o_name': name,
            'n_name': n,
            'url': u
        })
    except NeedValidationError as e:
        print('Need Validation, wait 2 seconds')
        time.sleep(2)
        # retry
        try:
            n, u = search_company(name, cookies, headers)
            result.append({
                'o_name': name,
                'n_name': n,
                'url': u
            })
        except Exception as e:
            with open('Big_Customer_result.json', 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False)
            raise e
    except Exception as e:
        with open('Big_Customer_result.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False)
        raise e

with open('Big_Customer_result.json', 'w', encoding='utf-8') as f:
    json.dump(result, f, ensure_ascii=False)
