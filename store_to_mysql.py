# -*- coding: utf-8 -*-
"""
Created on Tue May 22 17:56:34 2018

@author: storm

将json文件存入mysql
"""

import configparser
import datetime
import glob
import hashlib
import json
import os
import re

import pymysql
import pymongo

from tool.convert_to_number import convert_to_number
from tool.convert_to_date import convert_to_date
from tool.changeinfo import change_changeinfo


def store_listed_company(qichacha, eastmoney, cninfo, conn, cursor):
    data = qichacha
    eastmoney_data = eastmoney

    print('storing', data['companyName'])

    # 客户表 c_client
    c_id = re.search(r'firm_(\w+).html', qichacha['url']).group(1)
    name = data['companyName'].replace('（', '(').replace('）', ')')  # 公司名称以企查查为准 替换为英文括号
    state = data['baseInfo']['company_status']
    uscc = data['baseInfo']['credit_code']
    organ_num = data['baseInfo']['organization_num']
    enterprise_type = data['baseInfo']['company_type']
    deadline_begin = data['baseInfo']['business_limitation']
    deadline_end = data['baseInfo']['business_limitation']
    reg_office = data['baseInfo']['registration_authority']

    engl_name = eastmoney_data['survey']['base_info']['ywmc']
    used_name = eastmoney_data['survey']['base_info']['cym']
    is_listed = 1
    a_code = eastmoney_data['survey']['base_info']['agdm']
    a_name = eastmoney_data['survey']['base_info']['agjc']
    b_code = eastmoney_data['survey']['base_info']['bgdm']
    b_name = eastmoney_data['survey']['base_info']['bgjc']
    h_code = eastmoney_data['survey']['base_info']['hgdm']
    h_name = eastmoney_data['survey']['base_info']['hgjc']
    # 法人代表ID
    legal_name = eastmoney_data['survey']['base_info']['frdb']
    money = eastmoney_data['survey']['base_info']['zczb']   # double
    estab_time = eastmoney_data['survey']['about_issue']['clrq']    # date
    reg_address = eastmoney_data['survey']['base_info']['zcdz']
    busin_scope = eastmoney_data['survey']['base_info']['jyfw'][:1000]
    sfc = eastmoney_data['survey']['base_info']['sszjhhy']  # char(4)
    phone = eastmoney_data['survey']['base_info']['lxdh']
    email = eastmoney_data['survey']['base_info']['dzxx']
    fax = eastmoney_data['survey']['base_info']['cz']
    url = eastmoney_data['survey']['base_info']['gswz']
    off_address = eastmoney_data['survey']['base_info']['bgdz']
    region = eastmoney_data['survey']['base_info']['qy']
    postal_code = eastmoney_data['survey']['base_info']['yzbm']
    employee = eastmoney_data['survey']['base_info']['gyrs']
    management = eastmoney_data['survey']['base_info']['glryrs']
    law_office = eastmoney_data['survey']['base_info']['lssws']
    accounting_office = eastmoney_data['survey']['base_info']['kjssws']
    abstract = eastmoney_data['survey']['base_info']['gsjj'].strip()

    sql = """insert into c_client (c_id, name, engl_name, used_name, is_listed, a_code, a_name, b_code, b_name, h_code, h_name, state, 
        uscc, organ_num, enterprise_type, legal_name, money, estab_time, deadline_begin, deadline_end, reg_office, reg_address, busin_scope, 
        sfc, phone, email, fax, url, off_address, region, postal_code, employee, management, law_office, accounting_office, abstract)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.execute(sql, (c_id, name, engl_name,used_name, is_listed, a_code, a_name, b_code, b_name, h_code, h_name, state, uscc, organ_num, enterprise_type, 
        legal_name, money, estab_time, deadline_begin, deadline_end, reg_office, reg_address, busin_scope, sfc, phone, email, fax, url, 
        off_address, region, postal_code, employee, management, law_office, accounting_office, abstract))

    # 十大股东表 c_top_shaholder
    top10_holders = []
    for holders in eastmoney_data['holders']['top10_holders']:
        deadline = holders['rq']
        id_holder = 0
        for holder in holders['sdgd']:
            id_holder += 1
            shaholder_id = ''
            shaholder_name = holder['gdmc']
            number = convert_to_number(holder['cgs'])
            rate = holder['zltgbcgbl']
            shares_type = holder['gflx']

            top10_holders.append((c_id, deadline, id_holder, shaholder_id, shaholder_name, number, rate, shares_type))

    sql = """insert into c_top_shaholder (c_id, deadline, id, shaholder_id, shaholder_name, number, rate, shares_type)
        values (%s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.executemany(sql, top10_holders)

    # 十大流通股东表 c_ltop_shaholder
    top10_lt_holders = []
    for holders in eastmoney_data['holders']['top10_circulation_holders']:
        deadline = holders['rq']
        id_holder = 0
        for holder in holders['sdltgd']:
            id_holder += 1
            shaholder_id = ''
            shaholder_name = holder['gdmc']
            number = convert_to_number(holder['cgs'])
            rate = holder['zltgbcgbl']
            shares_type = holder['gflx']
            shaholder_type = holder['gdxz']

            top10_lt_holders.append((c_id, deadline, id_holder, shaholder_id, shaholder_name, number, rate, shares_type, shaholder_type))

    sql = """insert into c_ltop_shaholder (c_id, deadline, id, shaholder_id, shaholder_name, number, rate, shares_type, shaholder_type)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.executemany(sql, top10_lt_holders)

    # 主营业务表 c_key_business
    main_businesses = []
    for mb_out in eastmoney_data['business']:
        deadline = mb_out['date']
        id_mb = 0
        for mb in mb_out['industry']:
            id_mb += 1
            classify = '行业'
            type_mb = mb['zygc']
            income = convert_to_number(mb['zysr'])
            inc_rate = mb['srbl']
            cost = convert_to_number(mb['zycb'])
            cost_rate = mb['cbbl']
            profit = convert_to_number(mb['zylr'])
            pro_rate = mb['lrbl']
            mon_rate = mb['mll']

            main_businesses.append((c_id, deadline, id_mb, classify, type_mb,
                                    income, inc_rate, cost, cost_rate, profit, pro_rate, mon_rate))

        for mb in mb_out['area']:
            id_mb += 1
            classify = '地区'
            type_mb = mb['zygc']
            income = convert_to_number(mb['zysr'])
            inc_rate = mb['srbl']
            cost = convert_to_number(mb['zycb'])
            cost_rate = mb['cbbl']
            profit = mb['zylr']
            pro_rate = mb['lrbl']
            mon_rate = mb['mll']

            main_businesses.append((c_id, deadline, id_mb, classify, type_mb,
                                    income, inc_rate, cost, cost_rate, profit, pro_rate, mon_rate))

        for mb in mb_out['production']:
            id_mb += 1
            classify = '产品'
            type_mb = mb['zygc']
            income = convert_to_number(mb['zysr'])
            inc_rate = mb['srbl']
            cost = convert_to_number(mb['zycb'])
            cost_rate = mb['cbbl']
            profit = convert_to_number(mb['zylr'])
            pro_rate = mb['lrbl']
            mon_rate = mb['mll']

            main_businesses.append((c_id, deadline, id_mb, classify, type_mb,
                                    income, inc_rate, cost, cost_rate, profit, pro_rate, mon_rate))

    sql = """insert into c_key_business (c_id, deadline, id, classify, type, income, inc_rate, cost, cost_rate, profit, pro_rate, mon_rate)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.executemany(sql, main_businesses)

    # 对外投资表 c_investment
    investments = []
    for investment in data['investments']:
        i_id = re.search(r'firm_(\w+).html', investment['url']).group(1)
        name = investment['name']
        capital = investment['registered_capital'].replace(' ', '')
        count = ''
        rate = investment['invest_proportion']
        time = investment['registered_time']
        type_i = investment['company_status']

        investments.append((c_id, i_id, name, capital, count, rate, time, type_i))

    sql = """insert into c_investment (c_id, i_id, name, capital, count, rate, time, type)
    values (%s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.executemany(sql, investments)

    # 高管信息表 e_executive，客户高管表 client_executive
    # 如果不存在，新增
    # 如果存在，且信息不全（标志位？）则补充
    # 如果存在，信息全面，则跳过
    managers = []
    client_managers = []
    for manager in eastmoney_data['managers']:
        e_id = ''
        c_name = data['companyName']
        e_name = manager['name']
        age = manager['age']
        gender = manager['sex']
        education = manager['education']
        abstract = manager['brief']
        post = manager['position']
        serve_begin = manager['start_date']

        managers.append((e_id, c_id, c_name, e_name, age, gender, education, abstract))
        client_managers.append((c_id, e_id, e_name, post, serve_begin))

    sql = """insert into e_executive (e_id, c_id, c_name, e_name, age, gender, education, abstract)
    values (%s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.executemany(sql, managers)

    sql = """insert into client_executive (c_id, e_id, e_name, post, serve_begin)
    values (%s, %s, %s, %s, %s)"""

    cursor.executemany(sql, client_managers)

    # 变更信息表 c_changeinfo
    changeinfos = []
    for changeinfo in data['changeInfos']:
        if len(changeinfo['before']) > 2000 or len(changeinfo['after']) > 2000:
            continue
        time = changeinfo['time']
        item = changeinfo['item']
        before = change_changeinfo(changeinfo['before'])
        after = change_changeinfo(changeinfo['after'])

        changeinfos.append((c_id, time, item, before, after))

    sql = """insert into c_changeinfo (`c_id`, `time`, `item`, `before`, `after`)
    values (%s, %s, %s, %s, %s)"""

    cursor.executemany(sql, changeinfos)

    # 股票信息表 shares
    cninfo_data = cninfo

    s_id = eastmoney_data['code']
    s_name = eastmoney_data['name']
    security_type = eastmoney_data['survey']['base_info']['zqlb']
    list_date = eastmoney_data['survey']['about_issue']['ssrq']
    address = eastmoney_data['survey']['base_info']['ssjys']
    pe = eastmoney_data['survey']['about_issue']['fxsyl']
    issue_time = eastmoney_data['survey']['about_issue']['wsfxrq']
    issue_type = eastmoney_data['survey']['about_issue']['fxfs']
    face_value = eastmoney_data['survey']['about_issue']['mgmz']
    issue_count = convert_to_number(eastmoney_data['survey']['about_issue']['fxl']) / 10000
    issue_price = eastmoney_data['survey']['about_issue']['mgfxj']
    issue_cost = convert_to_number(eastmoney_data['survey']['about_issue']['fxfy']) / 10000
    issue_tot_market = convert_to_number(eastmoney_data['survey']['about_issue']['fxzsz']) / 10000
    raise_funds = convert_to_number(eastmoney_data['survey']['about_issue']['mjzjje']) / 10000
    open_price = eastmoney_data['survey']['about_issue']['srkpj']
    end_price = eastmoney_data['survey']['about_issue']['srspj']
    turnover_rate = eastmoney_data['survey']['about_issue']['srhsl']
    max_price = eastmoney_data['survey']['about_issue']['srzgj']
    under_rate = eastmoney_data['survey']['about_issue']['wxpszql']
    price_rate = eastmoney_data['survey']['about_issue']['djzql']

    lead_underwriter = cninfo_data['main_underwriter']
    recommender = cninfo_data['list_sponsor']
    institution = cninfo_data['sponsor_institution']

    sql = """insert into shares (s_id, s_name, security_type, list_date, address, pe, issue_time, issue_type, 
    face_value, issue_count, issue_price, issue_cost, issue_tot_market, raise_funds, open_price, end_price, 
    turnover_rate, max_price, under_rate, price_rate, lead_underwriter, recommender, institution)
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.execute(sql, (s_id, s_name, security_type, list_date, address, pe, issue_time, issue_type,
                         face_value, issue_count, issue_price, issue_cost, issue_tot_market, raise_funds, open_price, end_price,
                         turnover_rate, max_price, under_rate, price_rate, lead_underwriter, recommender, institution))

    # 股票融资表 shares_financing
    shares_financing = []
    s_id = eastmoney_data['code']
    s_name = eastmoney_data['name']

    # 新发    
    issue_type = '新发'
    issue_date = eastmoney_data['survey']['about_issue']['wsfxrq']
    issue_price = convert_to_number(eastmoney_data['survey']['about_issue']['mgfxj'])
    issue_count = convert_to_number(eastmoney_data['survey']['about_issue']['fxl']) / 10000
    raise_funds = convert_to_number(eastmoney_data['survey']['about_issue']['mjzjje']) / 10000
    issue_way = eastmoney_data['survey']['about_issue']['fxfs']
    equity_registration_date = ''
    additional_listing_date = ''
    fund_arrival_date = ''
    dividend_date = ''
    allotment_plan = ''

    shares_financing.append((s_id, s_name, issue_type, issue_date, issue_price, issue_count, raise_funds, issue_way, 
            equity_registration_date, additional_listing_date, fund_arrival_date, dividend_date, allotment_plan))

    # 增发
    for add_detail in eastmoney_data['add_details']:
        issue_type = '增发'
        issue_date = convert_to_date(add_detail['add_date'])
        issue_price = convert_to_number(add_detail['add_price'])
        issue_count = convert_to_number(add_detail['act_num_of_add'])
        raise_funds = convert_to_number(add_detail['act_add_net_raise'])
        issue_way = add_detail['issue_means']
        equity_registration_date = convert_to_date(add_detail['record_date'])
        additional_listing_date = convert_to_date(add_detail['sec_offer_date'])
        fund_arrival_date = convert_to_date(add_detail['fund_trans_date'])

        dividend_date = ''
        allotment_plan = ''

        shares_financing.append((s_id, s_name, issue_type, issue_date, issue_price, issue_count, raise_funds, issue_way, 
            equity_registration_date, additional_listing_date, fund_arrival_date, dividend_date, allotment_plan))
    
    # 配股
    for allot_detail in eastmoney_data['allot_details']:
        issue_type = '配股'
        issue_date = convert_to_date(allot_detail['allot_date'])
        issue_price = convert_to_number(allot_detail['allot_price'])
        issue_count = convert_to_number(allot_detail['act_num_of_allot'])
        raise_funds = convert_to_number(allot_detail['act_allot_net_raise'])
        issue_way = ''
        equity_registration_date = convert_to_date(allot_detail['allot_date'])
        additional_listing_date = ''
        fund_arrival_date = ''
        dividend_date = convert_to_date(allot_detail['reference_date'])
        allotment_plan = allot_detail['allot_plan']

        shares_financing.append((s_id, s_name, issue_type, issue_date, issue_price, issue_count, raise_funds, issue_way, 
            equity_registration_date, additional_listing_date, fund_arrival_date, dividend_date, allotment_plan))

    sql = """insert into shares_financing
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.executemany(sql, shares_financing)

    # 股票质押公告表
    pledge_announcements = []
    for announcement in cninfo_data['pledge_announcement']:
        a_title = announcement['announcementTitle']
        a_time = announcement['announcementTime']
        a_url = announcement['adjunctUrl']
        a_type = announcement['adjunctType']
        a_size = announcement['adjunctSize']

        pledge_announcements.append((s_id, s_name, a_title, a_time, a_url, a_type, a_size))

    sql = """insert into shares_pledge
    values (%s, %s, %s, %s, %s, %s, %s)"""

    cursor.executemany(sql, pledge_announcements)

    # B股信息
    if eastmoney_data['b_stock_info']:

        # 股票信息表 shares
        s_id = eastmoney_data['b_stock_info']['code']
        s_name = eastmoney_data['b_stock_info']['name']
        security_type = eastmoney_data['b_stock_info']['stock_type']
        list_date = eastmoney_data['b_stock_info']['list_date']
        address = eastmoney_data['b_stock_info']['exchange']
        pe = eastmoney_data['b_stock_info']['pe']
        issue_time = eastmoney_data['b_stock_info']['issue_date']
        issue_type = eastmoney_data['b_stock_info']['issue_type']
        face_value = eastmoney_data['b_stock_info']['face_value']
        issue_count = convert_to_number(eastmoney_data['b_stock_info']['issue_count'])
        issue_price = eastmoney_data['b_stock_info']['issue_price']
        issue_cost = convert_to_number(eastmoney_data['b_stock_info']['issue_cost'])
        issue_tot_market = convert_to_number(eastmoney_data['b_stock_info']['issue_tot_market'])
        raise_funds = convert_to_number(eastmoney_data['b_stock_info']['raise_funds'])
        open_price = eastmoney_data['b_stock_info']['open_price']
        end_price = eastmoney_data['b_stock_info']['end_price']
        turnover_rate = eastmoney_data['b_stock_info']['turnover_rate']
        max_price = eastmoney_data['b_stock_info']['max_price']
        under_rate = eastmoney_data['b_stock_info']['under_rate']
        price_rate = eastmoney_data['b_stock_info']['price_rate']

        lead_underwriter = cninfo_data['main_underwriter']
        recommender = cninfo_data['list_sponsor']
        institution = cninfo_data['sponsor_institution']

        sql = """insert into shares (s_id, s_name, security_type, list_date, address, pe, issue_time, issue_type, 
        face_value, issue_count, issue_price, issue_cost, issue_tot_market, raise_funds, open_price, end_price, 
        turnover_rate, max_price, under_rate, price_rate, lead_underwriter, recommender, institution)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        cursor.execute(sql, (s_id, s_name, security_type, list_date, address, pe, issue_time, issue_type,
                             face_value, issue_count, issue_price, issue_cost, issue_tot_market, raise_funds, open_price, end_price,
                             turnover_rate, max_price, under_rate, price_rate, lead_underwriter, recommender, institution))

    short_name = eastmoney_data['name']
    b_id = eastmoney_data['code']

    # 资产负债表 fin_balance
    balance_sheets = []
    for b_s in eastmoney_data['balance_sheet']:
        report_date = b_s['REPORTDATE']
        statement_type = b_s['TYPE']
        repote_type = b_s['REPORTTYPE']
        monetary_fund = b_s['MONETARYFUND']
        settlement_provision = b_s['SETTLEMENTPROVISION']
        lend_fund = b_s['LENDFUND']
        fair_value_fin_ass = b_s['FVALUEFASSET']
        tran_fin_ass = b_s['TRADEFASSET']
        define_fair_value_fin_ass = b_s['DEFINEFVALUEFASSET']
        bill_rec = b_s['BILLREC']
        account_rec = b_s['ACCOUNTREC']
        advance_pay = b_s['ADVANCEPAY']
        prem_rec = b_s['PREMIUMREC']
        rein_rec = b_s['RIREC']
        rein_contract_rec = b_s['RICONTACTRESERVEREC']
        interest_rec = b_s['INTERESTREC']
        dividend_rec = b_s['DIVIDENDREC']
        other_rec = b_s['OTHERREC']
        tax_rebate_export_rec = b_s['EXPORTREBATEREC']
        subsidies_rec = b_s['SUBSIDYREC']
        internal_rec = b_s['INTERNALREC']
        buy_back_sale_fin_ass = b_s['BUYSELLBACKFASSET']
        inventory = b_s['INVENTORY']
        divided_held_sale_ass = b_s['CLHELDSALEASS']
        non_curr_ass_one_year = b_s['NONLASSETONEYEAR']
        other_curr_ass = b_s['OTHERLASSET']
        sum_curr_ass = b_s['SUMLASSET']
        loan_advances = b_s['LOANADVANCES']
        saleable_fin_ass = b_s['SALEABLEFASSET']
        hold_maturity_inv = b_s['HELDMATURITYINV']
        lt_rec = b_s['LTREC']
        lt_equity_inv = b_s['LTEQUITYINV']
        estate_inv = b_s['ESTATEINVEST']
        fixed_ass = b_s['FIXEDASSET']
        constructiong_project = b_s['CONSTRUCTIONPROGRESS']
        engineer_material = b_s['CONSTRUCTIONMATERIAL']
        fixed_ass_clean = b_s['LIQUIDATEFIXEDASSET']
        productive_biological_ass = b_s['PRODUCTBIOLOGYASSET']
        oil_gas_ass = b_s['OILGASASSET']
        intangble_ass = b_s['INTANGIBLEASSET']
        develop_exp = b_s['DEVELOPEXP']
        goodwill = b_s['GOODWILL']
        lt_defe_exp = b_s['LTDEFERASSET']
        defe_tax_ass = b_s['DEFERINCOMETAXASSET']
        other_non_curr_ass = b_s['OTHERNONLASSET']
        sum_non_curr_ass = b_s['SUMNONLASSET']
        sum_ass = b_s['SUMASSET']
        st_borrow = b_s['STBORROW']
        borrow_f_cent_bank = b_s['BORROWFROMCBANK']
        deposit = b_s['DEPOSIT']
        loan_f_other_bank = b_s['BORROWFUND']
        fair_value_fin_liab = b_s['FVALUEFLIAB']
        tran_fin_liab = b_s['TRADEFLIAB']
        fin_liab_designated_fair_value = b_s['DEFINEFVALUEFLIAB']
        bill_pay = b_s['BILLPAY']
        account_pay = b_s['ACCOUNTPAY']
        advance_rec = b_s['ADVANCERECEIVE']
        sell_buy_back_fin_ass = b_s['SELLBUYBACKFASSET']
        hand_fee_comm = b_s['COMMPAY']
        salary_pay = b_s['SALARYPAY']
        tax_pay = b_s['TAXPAY']
        int_pay = b_s['INTERESTPAY']
        dividends_pay = b_s['DIVIDENDPAY']
        rein_account_pay = b_s['RIPAY']
        internal_account_pay = b_s['INTERNALPAY']
        other_pay = b_s['OTHERPAY']
        expected_curr_liab = b_s['ANTICIPATELLIAB']
        ins_contract_res = b_s['CONTACTRESERVE']
        agent_buy_sell_security = b_s['AGENTTRADESECURITY']
        agent_underwriting_security = b_s['AGENTUWSECURITY']
        defe_income_one_year = b_s['DEFERINCOMEONEYEAR']
        st_bonds_pay = b_s['STBONDREC']
        divi_sale_liab = b_s['CLHELDSALELIAB']
        non_curr_liab_one_year = b_s['NONLLIABONEYEAR']
        other_curr_liab = b_s['OTHERLLIAB']
        sum_curr_liab = b_s['SUMLLIAB']
        lt_borrow = b_s['LTBORROW']
        bond_pay = b_s['BONDPAY']
        preferred_stock_bond = b_s['PREFERSTOCBOND']
        sustain_bond = b_s['SUSTAINBOND']
        lt_account_pay = b_s['LTACCOUNTPAY']
        lt_salary_staff_pay = b_s['LTSALARYPAY']
        special_pay = b_s['SPECIALPAY']
        expected_liab = b_s['ANTICIPATELIAB']
        defe_income = b_s['DEFERINCOME']
        defe_income_tax_liab = b_s['DEFERINCOMETAXLIAB']
        other_non_liab = b_s['OTHERNONLLIAB']
        sum_non_liab = b_s['SUMNONLLIAB']
        sum_liab = b_s['SUMLIAB']
        share_capital = b_s['SHARECAPITAL']
        other_equity_tool = b_s['OTHEREQUITY']
        preferred_stock = b_s['PREFERREDSTOCK']
        sustain_bond_debt = b_s['SUSTAINABLEDEBT']
        other_equity_tool_other = b_s['OTHEREQUITYOTHER']
        capital_res = b_s['CAPITALRESERVE']
        treasury_stock = b_s['INVENTORYSHARE']
        special_res = b_s['SPECIALRESERVE']
        surplus_res = b_s['SURPLUSRESERVE']
        general_risk_prepare = b_s['GENERALRISKPREPARE']
        undetermined_inv_loss = b_s['UNCONFIRMINVLOSS']
        retaind_earn = b_s['RETAINEDEARNING']
        plan_cash_dividend = b_s['PLANCASHDIVI']
        fore_cur_statement_diff = b_s['DIFFCONVERSIONFC']
        sum_parent_equity = b_s['SUMPARENTEQUITY']
        min_she_equity = b_s['MINORITYEQUITY']
        sum_she_equity = b_s['SUMSHEQUITY']
        sum_liab_she_equity = b_s['SUMLIABSHEQUITY']
        derivative_fin_ass = ''
        apportioned_cost = ''
        deal_profit_loss_curr_ass = ''
        public_welfare_biol_ass = ''
        devel_exp = ''
        advance_cost = ''
        non_curr_liab = ''
        leasehold = ''

        balance_sheets.append((c_id, report_date, short_name, b_id, statement_type, repote_type, monetary_fund, settlement_provision,
                               lend_fund, fair_value_fin_ass, tran_fin_ass, define_fair_value_fin_ass, bill_rec, account_rec, advance_pay,
                               prem_rec, rein_rec, rein_contract_rec, interest_rec, dividend_rec, other_rec, tax_rebate_export_rec,
                               subsidies_rec, internal_rec, buy_back_sale_fin_ass, inventory, divided_held_sale_ass, non_curr_ass_one_year,
                               other_curr_ass, sum_curr_ass, loan_advances, saleable_fin_ass, hold_maturity_inv, lt_rec, lt_equity_inv,
                               estate_inv, fixed_ass, constructiong_project, engineer_material, fixed_ass_clean, productive_biological_ass,
                               oil_gas_ass, intangble_ass, develop_exp, goodwill, lt_defe_exp, defe_tax_ass, other_non_curr_ass, sum_non_curr_ass,
                               sum_ass, st_borrow, borrow_f_cent_bank, deposit, loan_f_other_bank, fair_value_fin_liab, tran_fin_liab,
                               fin_liab_designated_fair_value, bill_pay, account_pay, advance_rec, sell_buy_back_fin_ass, hand_fee_comm,
                               salary_pay, tax_pay, int_pay, dividends_pay, rein_account_pay, internal_account_pay, other_pay, expected_curr_liab,
                               ins_contract_res, agent_buy_sell_security, agent_underwriting_security, defe_income_one_year, st_bonds_pay,
                               divi_sale_liab, non_curr_liab_one_year, other_curr_liab, sum_curr_liab, lt_borrow, bond_pay, preferred_stock_bond,
                               sustain_bond, lt_account_pay, lt_salary_staff_pay, special_pay, expected_liab, defe_income, defe_income_tax_liab,
                               other_non_liab, sum_non_liab, sum_liab, share_capital, other_equity_tool, preferred_stock, sustain_bond_debt,
                               other_equity_tool_other, capital_res, treasury_stock, special_res, surplus_res, general_risk_prepare, undetermined_inv_loss,
                               retaind_earn, plan_cash_dividend, fore_cur_statement_diff, sum_parent_equity, min_she_equity, sum_she_equity,
                               sum_liab_she_equity, derivative_fin_ass, apportioned_cost, deal_profit_loss_curr_ass, public_welfare_biol_ass,
                               devel_exp, advance_cost, non_curr_liab, leasehold))

    sql = """insert into fin_balance
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.executemany(sql, balance_sheets)

    # 利润表 fin_income
    income_statements = []
    for i_s in eastmoney_data['income_statement']:
        report_date = i_s['REPORTDATE']
        statement_type = i_s['TYPE']
        repote_type = i_s['REPORTTYPE']
        tot_operate_rev = i_s['TOTALOPERATEREVE']
        operate_rev = i_s['OPERATEREVE']
        int_income = i_s['INTREVE']
        earn_prem = i_s['PREMIUMEARNED']
        fee_comm_income = i_s['COMMREVE']
        other_business_income = i_s['OTHERREVE']
        total_operate_exp = i_s['TOTALOPERATEEXP']
        operate_exp = i_s['OPERATEEXP']
        interest_expe = i_s['INTEXP']
        fee_comm_exp = i_s['COMMEXP']
        r_d_cost = i_s['RDEXP']
        surrender_prem = i_s['SURRENDERPREMIUM']
        net_reimbursement = i_s['NETINDEMNITYEXP']
        net_insurance_cont = i_s['NETCONTACTRESERVE']
        insurance_policy = i_s['POLICYDIVIEXP']
        rein_cost = i_s['RIEXP']
        other_business_cost = i_s['OTHEREXP']
        operate_tax = i_s['OPERATETAX']
        sale_exp = i_s['SALEEXP']
        manage_exp = i_s['MANAGEEXP']
        fin_exp = i_s['FINANCEEXP']
        ass_impairment_loss = i_s['ASSETDEVALUELOSS']
        fair_value_income = i_s['FVALUEINCOME']
        inv_income = i_s['INVESTINCOME']
        inv_join_income = i_s['INVESTJOINTINCOME']
        exchange_income = i_s['EXCHANGEINCOME']
        operate_profig = i_s['OPERATEPROFIT']
        non_operate_profig = i_s['NONOPERATEREVE']
        non_curr_ass_gain = i_s['NONLASSETREVE']
        non_operate_exp = i_s['NONOPERATEEXP']
        non_curr_ass_net_loss = i_s['NONLASSETNETLOSS']
        sum_profit = i_s['SUMPROFIT']
        income_tax = i_s['INCOMETAX']
        profit_take = ''
        net_profit = i_s['NETPROFIT']
        parent_net_profit = i_s['PARENTNETPROFIT']
        min_income = i_s['MINORITYINCOME']
        deduct_profit_loss_net_profit = i_s['KCFJCXSYJLR']
        pre_share_income = ''
        basic_earn_per_share = i_s['BASICEPS']
        dilute_earn_per_share = i_s['DILUTEDEPS']
        other_compre_income = i_s['OTHERCINCOME']
        parent_other_compre_income = i_s['PARENTOTHERCINCOME']
        minority_other_compre_income = i_s['MINORITYOTHERCINCOME']
        sum_compre_income = i_s['SUMCINCOME']
        parent_compre_income = i_s['PARENTCINCOME']
        min_compre_income = i_s['MINORITYCINCOME']

        income_statements.append((c_id, report_date, short_name, b_id, statement_type, repote_type, tot_operate_rev, operate_rev, int_income,
                                  earn_prem, fee_comm_income, other_business_income, total_operate_exp, operate_exp, interest_expe, fee_comm_exp,
                                  r_d_cost, surrender_prem, net_reimbursement, net_insurance_cont, insurance_policy, rein_cost, other_business_cost,
                                  operate_tax, sale_exp, manage_exp, fin_exp, ass_impairment_loss, fair_value_income, inv_income, inv_join_income,
                                  exchange_income, operate_profig, non_operate_profig, non_curr_ass_gain, non_operate_exp, non_curr_ass_net_loss,
                                  sum_profit, income_tax, profit_take, net_profit, parent_net_profit, min_income, deduct_profit_loss_net_profit,
                                  pre_share_income, basic_earn_per_share, dilute_earn_per_share, other_compre_income, parent_other_compre_income,
                                  minority_other_compre_income, sum_compre_income, parent_compre_income, min_compre_income))

    sql = """insert into fin_income
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.executemany(sql, income_statements)

    # 现金流量表 fin_cash_flow
    cash_flow_statements = []
    for c_f_s in eastmoney_data['cash_flow_statement']:
        report_date = c_f_s['REPORTDATE']
        statement_type = c_f_s['TYPE']
        repote_type = c_f_s['REPORTTYPE']
        sale_goods_service_rec = c_f_s['SALEGOODSSERVICEREC']
        ni_deposit = c_f_s['NIDEPOSIT']
        ni__borrow_f_cent_bank = c_f_s['NIBORROWFROMCBANK']
        ni__borrow_f_fin_institution = c_f_s['NIBORROWFROMFI']
        perm_rec = c_f_s['PREMIUMREC']
        net_rein_rec = c_f_s['NETRIREC']
        ni_insured_deposit_inv = c_f_s['NIINSUREDDEPOSITINV']
        ni_disposit_tran_fin_ass = c_f_s['NIDISPTRADEFASSET']
        interest_auxiliary_comm_rec = c_f_s['INTANDCOMMREC']
        ni_borrow_fund = c_f_s['NIBORROWFUND']
        nd_loan_advance = c_f_s['NDLOANADVANCES']
        ni_buy_back_fund = c_f_s['NIBUYBACKFUND']
        tax_return_rec = c_f_s['TAXRETURNREC']
        other_operate_rec = c_f_s['OTHEROPERATEREC']
        sum_operate_flow_in = c_f_s['SUMOPERATEFLOWIN']
        buy_goods_service_pay = c_f_s['BUYGOODSSERVICEPAY']
        ni_lloan_advance = c_f_s['NILOANADVANCES']
        ni_deposit_centl_bank = c_f_s['NIDEPOSITINCBANKFI']
        indemnity_pay = c_f_s['INDEMNITYPAY']
        interest_auxiliary_comm_pay = c_f_s['INTANDCOMMPAY']
        dividend_pay = c_f_s['DIVIPAY']
        employee_pay = c_f_s['EMPLOYEEPAY']
        tax_pay = c_f_s['TAXPAY']
        other_operate_pay = c_f_s['OTHEROPERATEPAY']
        sum_operate_flow_out = c_f_s['SUMOPERATEFLOWOUT']
        net_operate_cash_flow = c_f_s['NETOPERATECASHFLOW']
        disposal_inv_rec = c_f_s['DISPOSALINVREC']
        inv_income_rec = c_f_s['INVINCOMEREC']
        disposal_fixed_immaterial_ass_rec = c_f_s['DISPFILASSETREC']
        disposal_sub_rec = c_f_s['DISPSUBSIDIARYREC']
        reduce_pledge_get_deposit = c_f_s['REDUCEPLEDGETDEPOSIT']
        other_inv_rec = c_f_s['OTHERINVREC']
        sum_inv_folw_in = c_f_s['SUMINVFLOWIN']
        buy_fil_ass = c_f_s['BUYFILASSETPAY']
        inv_pay = c_f_s['INVPAY']
        ni_pledge_loan = c_f_s['NIPLEDGELOAN']
        get_sub_pay = c_f_s['GETSUBSIDIARYPAY']
        add_pledge_deposit_pay = c_f_s['ADDPLEDGETDEPOSIT']
        other_inv_pay = c_f_s['OTHERINVPAY']
        sum_inv_folw_out = c_f_s['SUMINVFLOWOUT']
        net_inv_cash_flow = c_f_s['NETINVCASHFLOW']
        accept_inv_rec = c_f_s['ACCEPTINVREC']
        sub_accept = c_f_s['SUBSIDIARYACCEPT']
        loan_rec = c_f_s['LOANREC']
        issue_bond_rec = c_f_s['ISSUEBONDREC']
        other_fin_rec = c_f_s['OTHERFINAREC']
        sum_fin_rec = c_f_s['SUMFINAFLOWIN']
        repay_debt_pay = c_f_s['REPAYDEBTPAY']
        dividend_profit_or_interest_pay = c_f_s['DIVIPROFITORINTPAY']
        sub_pay = c_f_s['SUBSIDIARYPAY']
        buy_sub_pay = c_f_s['BUYSUBSIDIARYPAY']
        other_fin_pay = c_f_s['OTHERFINAPAY']
        sunsidiary_reduct_capital = c_f_s['SUBSIDIARYREDUCTCAPITAL']
        sum_fin_flow_out = c_f_s['SUMFINAFLOWOUT']
        net_fin_cash_flow = c_f_s['NETFINACASHFLOW']
        effect_exchange_rate = c_f_s['EFFECTEXCHANGERATE']
        ni_cash_equi = c_f_s['NICASHEQUI']
        cash_equi_begin = c_f_s['CASHEQUIBEGINNING']
        cash_equi_end = c_f_s['CASHEQUIENDING']

        cash_flow_statements.append((c_id, report_date, short_name, b_id, statement_type, repote_type, sale_goods_service_rec, ni_deposit,
                                     ni__borrow_f_cent_bank, ni__borrow_f_fin_institution, perm_rec, net_rein_rec, ni_insured_deposit_inv,
                                     ni_disposit_tran_fin_ass, interest_auxiliary_comm_rec, ni_borrow_fund, nd_loan_advance, ni_buy_back_fund,
                                     tax_return_rec, other_operate_rec, sum_operate_flow_in, buy_goods_service_pay, ni_lloan_advance,
                                     ni_deposit_centl_bank, indemnity_pay, interest_auxiliary_comm_pay, dividend_pay, employee_pay, tax_pay,
                                     other_operate_pay, sum_operate_flow_out, net_operate_cash_flow, disposal_inv_rec, inv_income_rec,
                                     disposal_fixed_immaterial_ass_rec, disposal_sub_rec, reduce_pledge_get_deposit, other_inv_rec, sum_inv_folw_in,
                                     buy_fil_ass, inv_pay, ni_pledge_loan, get_sub_pay, add_pledge_deposit_pay, other_inv_pay, sum_inv_folw_out,
                                     net_inv_cash_flow, accept_inv_rec, sub_accept, loan_rec, issue_bond_rec, other_fin_rec, sum_fin_rec,
                                     repay_debt_pay, dividend_profit_or_interest_pay, sub_pay, buy_sub_pay, other_fin_pay, sunsidiary_reduct_capital,
                                     sum_fin_flow_out, net_fin_cash_flow, effect_exchange_rate, ni_cash_equi, cash_equi_begin, cash_equi_end))

    sql = """insert into fin_cash_flow
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
             %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.executemany(sql, cash_flow_statements)

    conn.commit()

    print('stored successfully')


def store_not_listed_company(qichacha, conn, cursor):
    data = qichacha

    print('storing', data['companyName'])

    # 客户表 c_client
    c_id = re.search(r'firm_(\w+).html', data['url']).group(1)
    name = data['companyName'].replace('（', '(').replace('）', ')')
    engl_name = data['baseInfo']['english_name']
    used_name = data['baseInfo']['used_name']
    is_listed = 0
    # 法人代表ID
    legal_name = data['baseInfo']['legal_person']
    money = data['baseInfo']['registered_capital']
    estab_time = data['baseInfo']['registered_time']
    reg_address = data['baseInfo']['registered_address']
    busin_scope = data['baseInfo']['business_scope'][:1000]
    sfc = data['baseInfo']['industry']  # char(4)
    phone = data['overview']['phone']
    email = data['overview']['email']
    url = data['overview']['website']
    off_address = data['overview']['address']
    region = data['baseInfo']['area']  # char(4)
    employee = data['baseInfo']['staff_size']
    state = data['baseInfo']['company_status']
    uscc = data['baseInfo']['credit_code']
    organ_num = data['baseInfo']['organization_num']
    enterprise_type = data['baseInfo']['company_type']
    deadline_begin = data['baseInfo']['business_limitation']    # date
    deadline_end = data['baseInfo']['business_limitation']  # date
    reg_office = data['baseInfo']['registration_authority']

    sql = """insert into c_client (c_id, name, engl_name, used_name, is_listed, state, uscc, organ_num, enterprise_type, legal_name, money, estab_time, 
    deadline_begin, deadline_end, reg_office, reg_address, busin_scope, sfc, phone, email, url, off_address, region, employee)
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.execute(sql, (c_id, name, engl_name, used_name, is_listed, state, uscc, organ_num, enterprise_type, legal_name, money, estab_time, deadline_begin,
                         deadline_end, reg_office, reg_address, busin_scope, sfc, phone, email, url, off_address, region, employee))

    # 股东表 c_shaholder
    holders = []
    for holder in data['holders']:
        time = datetime.date.today().isoformat()
        m = re.search(r'firm_(\w+).html', holder['url'])
        if m:
            shaholder_id = m.group(1)
        else:
            shaholder_id = ''
        shaholder_name = holder['name']
        invest_rate = holder['proportion']
        sub_funding = holder['amount']
        sub_funding_date = holder['time']
        r_p_funding = holder['r_p_amount']
        r_p_funding_date = holder['r_p_time']

        holders.append((c_id, time, shaholder_id, shaholder_name, invest_rate, sub_funding, sub_funding_date, r_p_funding, r_p_funding_date))

    sql = """insert into c_shaholder (c_id, time, shaholder_id, shaholder_name, invest_rate, sub_funding, sub_funding_date, r_p_funding, r_p_funding_date)
    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.executemany(sql, holders)

    # 对外投资表 c_investment
    investments = []
    for investment in data['investments']:
        i_id = re.search(r'firm_(\w+).html', investment['url']).group(1)
        name = investment['name']
        capital = investment['registered_capital'].replace(' ', '')
        count = ''
        rate = investment['invest_proportion']
        time = investment['registered_time']
        type_i = investment['company_status']

        investments.append((c_id, i_id, name, capital, count, rate, time, type_i))

    sql = """insert into c_investment (c_id, i_id, name, capital, count, rate, time, type)
    values (%s, %s, %s, %s, %s, %s, %s, %s)"""

    cursor.executemany(sql, investments)

    # 高管信息表 e_executive, 客户高管表 client_executive
    # 如果不存在，新增
    # 如果存在，且信息不全（标志位？）则补充
    # 如果存在，信息全面，则跳过
    managers = []
    client_managers = []
    for manager in data['managers']:
        e_id = ''
        c_name = data['companyName']
        e_name = manager['name']
        post = manager['position']

        managers.append((e_id, c_id, c_name, e_name))
        client_managers.append((c_id, e_id, e_name, post))

    sql = """insert into e_executive (e_id, c_id, c_name, e_name)
    values (%s, %s, %s, %s)"""

    cursor.executemany(sql, managers)

    sql = """insert into client_executive (c_id, e_id, e_name, post)
    values (%s, %s, %s, %s)"""

    cursor.executemany(sql, client_managers)

    # 变更信息表 c_changeinfo
    changeinfos = []
    for changeinfo in data['changeInfos']:
        if len(changeinfo['before']) > 2000 or len(changeinfo['after']) > 2000:
            continue
        time = changeinfo['time']
        item = changeinfo['item']
        before = change_changeinfo(changeinfo['before'])
        after = change_changeinfo(changeinfo['after'])

        changeinfos.append((c_id, time, item, before, after))

    sql = """insert into c_changeinfo (`c_id`, `time`, `item`, `before`, `after`)
    values (%s, %s, %s, %s, %s)"""

    cursor.executemany(sql, changeinfos)

    conn.commit()

    print('stored successfully')


def store(qichacha, eastmoney, cninfo, conn, cursor):
    c_id = re.search(r'firm_(\w+).html', qichacha['url']).group(1)

    sql = """select c_id from c_client where c_id='{}'""".format(c_id)
    cursor.execute(sql)

    if cursor.fetchone():
        print(qichacha['companyName'], 'already in database')
    else:
        try:
            if eastmoney:
                store_listed_company(qichacha, eastmoney, cninfo, conn, cursor)
            else:
                store_not_listed_company(qichacha, conn, cursor)
        except Exception as e:
            conn.rollback()
            raise e


if __name__ == '__main__':
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

    try:
        crawl_mongodb_host = config['crawl_mongodb']['host']
        crawl_mongodb_port = config['crawl_mongodb']['port']
        crawl_mongodb_db = config['crawl_mongodb']['db']
        crawl_mongodb_col = config['crawl_mongodb']['collection']
        mongo_client = pymongo.MongoClient(host=crawl_mongodb_host, port=int(crawl_mongodb_port))
        mongo_collection = mongo_client[crawl_mongodb_db][crawl_mongodb_col]
    except Exception as e:
        print('Connect to crawl_mongodb failed')
        print(e)
        cursor.close()
        conn.close()
        exit()

    item = mongo_collection.find_one({'store_time': '', 'eastmoney': ''}, {'qichacha': 1, 'eastmoney': 1, 'cninfo': 1})

    store(item['qichacha'], item['eastmoney'], item['cninfo'], conn, cursor)

    cursor.close()
    conn.close()
