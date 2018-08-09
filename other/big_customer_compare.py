"""
比较两个大客户列表
"""

with open('BigCustomer.list', 'r', encoding='utf-8') as f:
    old_list = set([i.strip() for i in f.readlines()])

with open('BigCustomer2.list', 'r', encoding='utf-8') as f:
    new_list = set([i.strip() for i in f.readlines()])

old_and_new = old_list & new_list

old_not_new = old_list - new_list
new_not_old = new_list - old_list

total = old_list | new_list

with open('1.list', 'w', encoding='utf-8') as f:
    f.writelines([i+'\n' for i in old_and_new])
    
with open('2.list', 'w', encoding='utf-8') as f:
    f.writelines([i+'\n' for i in old_not_new])
    
with open('3.list', 'w', encoding='utf-8') as f:
    f.writelines([i+'\n' for i in new_not_old])
    
with open('4.list', 'w', encoding='utf-8') as f:
    f.writelines([i+'\n' for i in total])