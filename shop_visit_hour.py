#!/usr/local/bin/python3
# coding=utf8

import csv
from elasticsearch import Elasticsearch
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




def shop_visit_hour(  beg_date, end_date, mid ):

    shop_visit_hour_sql = """ select a.trade_date stat_date,a.mid,a.sid,
                            sum(case when a.order_hour=0 then a.num else 0 end) zero,
                            sum(case when a.order_hour=1 then a.num else 0 end) one,
                            sum(case when a.order_hour=2 then a.num else 0 end) two,
                            sum(case when a.order_hour=3 then a.num else 0 end) three,
                            sum(case when a.order_hour=4 then a.num else 0 end) four,
                            sum(case when a.order_hour=5 then a.num else 0 end) five,
                            sum(case when a.order_hour=6 then a.num else 0 end) six,
                            sum(case when a.order_hour=7 then a.num else 0 end) seven,
                            sum(case when a.order_hour=8 then a.num else 0 end) eight,
                            sum(case when a.order_hour=9 then a.num else 0 end) nine,
                            sum(case when a.order_hour=10 then a.num else 0 end) ten,
                            sum(case when a.order_hour=11 then a.num else 0 end) eleven,
                            sum(case when a.order_hour=12 then a.num else 0 end) twelve,
                            sum(case when a.order_hour=13 then a.num else 0 end) thirteen,
                            sum(case when a.order_hour=14 then a.num else 0 end) fourteen,
                            sum(case when a.order_hour=15 then a.num else 0 end) fifteen,
                            sum(case when a.order_hour=16 then a.num else 0 end) sixteen,
                            sum(case when a.order_hour=17 then a.num else 0 end) seveteen,
                            sum(case when a.order_hour=18 then a.num else 0 end) eighteen,
                            sum(case when a.order_hour=19 then a.num else 0 end) nineteen,
                            sum(case when a.order_hour=20 then a.num else 0 end) twenty,
                            sum(case when a.order_hour=21 then a.num else 0 end) twentyone,
                            sum(case when a.order_hour=22 then a.num else 0 end) twentytwo,
                            sum(case when a.order_hour=23 then a.num else 0 end) twentythree 
                            from (SELECT trade_date,order_hour,mid,sid,count(gid) num  FROM mall{}.dwd_order_with_category WHERE  to_char(trade_date,'YYYY-MM-DD') BETWEEN '{}' AND  '{}' GROUP BY trade_date,order_hour,mid,sid) a  GROUP BY a.trade_date,a.mid,a.sid;"""

    del_shop_visit_hour = """ delete from public.dwa_shop_visit_hour_by_day where mid = {} and to_char(stat_date,'YYYY-MM-DD') between '{}' and '{}'; """


    sql = shop_visit_hour_sql.format(mid,beg_date,end_date)
    data = common.select_sql(sql)
    if data is None or len(data) == 0 :
        logger.warning("No data.")
        return -1

    del_sql = del_shop_visit_hour.format(mid,beg_date,end_date)
    common.execute_sql(del_sql)
    logger.info('mid={} {} 至 {} 数据已删除'.format(mid,beg_date,end_date))

    data = data.fillna(0)
    if common.insert_to_DB_from_DF(data, 'public.dwa_shop_visit_hour_by_day', 1000) == 1:
        logger.error('插入 public.dwa_shop_visit_hour_by_day 数据失败，mid= {}'.format(mid))

    logger.info('mid={},插入 {} 条数据'.format(mid,len(data)))
    return



if __name__ == '__main__':
    start_time = datetime.datetime.now()
    work_dir, connect_db, postgres_host, postgres_port, postgres_user, postgres_password, postgres_datebase = common.get_config()
    mall_id = common.get_mallID()
    # mall_id = ['5']
    # pools = common.get_CPU_num()
    logger = common.bigdata_logger('analyse.log')

    parser = argparse.ArgumentParser()
    parser.add_argument(dest = "data_date", help = '指定下载数据的时间范围 1:前一天 2:指定日期范围')
    parser.add_argument('-b', dest = "begin_date", help = '指定开始日期，2021-07-07')
    parser.add_argument('-e', dest = "end_date", help = '指定结束日期，2021-07-17')
    args = parser.parse_args()
    logger.info('dwa_shop_visit_hour_by_day starting ...')
    if args.data_date == '1':
        begin_date = (datetime.date.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
        end_date = begin_date
        for mid in mall_id:
            shop_visit_hour(begin_date, end_date, mid)  # 下载前日数据
    elif args.data_date == '2':
        if args.begin_date and args.end_date and args.begin_date <= args.end_date:
            for mid in mall_id:
                shop_visit_hour(args.begin_date, args.end_date, mid)  # 批量下载多日数据
        else:
            logger.error('开始日期[{}] 大于 结束日期[{}]，请重新输入日期范围！'.format(args.begin_date,args.end_date))
            parser.print_help()
            exit(1)
    else:
        parser.print_help()
        exit(1)

    logger.info("插入 public.dwa_shop_visit_hour_by_day duration: {0}".format(datetime.datetime.now() - start_time))
