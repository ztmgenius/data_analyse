#!/usr/local/bin/python3
# coding=utf8

import csv
import datetime
from time import strftime, localtime
import pandas as pd
import time
from multiprocessing import Pool
import os
import logging
import logging.handlers
import sys
import argparse
import common
import db
import json
import numpy as np
import public


def improt_bigdata_from_pg():
    pass

def import_check():
    mall_list = [2, 3, 5, 7, 9, 11, 18, 19, 21]
    mall_tbl_list = ["ods_allowance_info","ods_order_info","ods_payment_info","ods_refund_info","dwa_holiday_pay_stat",
                "dwa_mall_rfm_stat","dwa_prmcategory_user_stat","dwa_user_pay_stat","dwa_user_visit_category_prm",
                "dwa_user_visit_category_umall","dwa_user_visit_holiday","dwa_user_visit_hour","dwa_user_visit_shop_stat",
                "dwd_category_interval_prm","dwd_category_interval_umall","dwd_order_with_category","dwd_user_category_prm",
                "dwd_user_category_umall","dwd_user_consume_by_day","dwa_user_consume_by_7day","dwa_user_consume_by_1month",
                "dwa_user_consume_by_3month","dwa_user_consume_by_6month","dwa_user_consume_by_year","dwd_user_park_info",
                "dwd_user_refund_info","dwd_user_shop_interval","dwd_user_trade_date"]
    public_tbl_list = ["ods_god","ods_mall_vip","ods_ali_user_info","ods_ten_user_info","ods_app_god_detail","ods_expense_card",
                  "ods_vip_point","ods_expense_details","ods_credit","ods_voucher","ods_license_plate","ods_assistant_shop",
                  "ods_shop","ods_shop_category","ods_home_order","ods_ly_contract","ods_contract","ods_prm_category",
                  "ods_comment_info","ods_promotion_info","ods_vip_pay_info","ods_vip_card_info","ods_park_voucher",
                  "ods_bind_relation","ods_app_god_wechat","ods_complaint_info","ods_contract_fee_plan","ods_shop_entry_info",
                  "ods_car_pay","dwd_shop_sales","dwa_shop_stat_avg","dwa_user_interaction","dwd_promotion_ecard_pay",
                  "dwd_promtion_stat_by_gid","dwd_user_info","dwd_shop_prm_category","dwd_promotion_provide_stat",
                  "dwd_promotion_use_stat","ods_shop_entry_info"]
    bigdata_db = db.database('bigdata_db')
    all_table =  mall_tbl_list + public_tbl_list
    for table_name in all_table:
        for mall_id in mall_list:
            if table_name in mall_tbl_list:
                schema = f"mall{mall_id}"
            else:
                schema = "public"
            check_sql = f""" select to_char(current_date - interval '1 d','yyyy-mm-dd') as date,count(*) as nums from {schema}.{table_name} where ctime > current_date - interval '1 d'; """
            check_res = bigdata_db.select_sql(check_sql)
            if check_res.nums[0] > 0:
                logger.info(f"""日期:{check_res.date[0]},表名:{schema}.{table_name},当日记录条数:{check_res.nums[0]}""")
            else:
                logger.warning(f"""日期:{check_res.date[0]},表名:{schema}.{table_name},当日无数据，请核实""")
            if schema == "public":
                break
            else:
                continue



if __name__ == '__main__':
    logger = public.bigdata_logger('database_check.log')

    import_check()