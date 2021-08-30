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




def shop_stat(beg_date, end_date, mid):

    shop_stat_sql = """ SELECT m.trade_date stat_date,m.mid,m.sid,s.area,m.sales_amount,m.order_num,m.pay_person ,n.refund_order,n.refund_amount,m.prm_category_1,m.prm_category_2 FROM 
    (SELECT	trade_date,	mid,sid,prm_category_1,	prm_category_2,	SUM ( origin_amount ) sales_amount,	COUNT ( DISTINCT gid ) pay_person,	COUNT ( order_no ) order_num
     FROM mall{}.dwd_order_with_category GROUP BY trade_date,mid,sid,prm_category_1,prm_category_2) m	
    LEFT JOIN   
    (SELECT a.trade_date,a.mid,a.sid,sum(origin_amount) refund_amount,count(order_no) refund_order FROM mall{}.dwd_order_with_category a WHERE EXISTS (SELECT 1 FROM mall{}.ods_refund_info b WHERE a.order_no=b.order_no)
     GROUP BY a.trade_date,	a.mid,a.sid) n	ON m.trade_date=n.trade_date AND m.mid=n.mid AND m.sid=n.sid
    LEFT JOIN	(SELECT sid,area FROM public.dwd_shop_prm_category ) s ON s.sid=m.sid
    where to_char(m.trade_date,'YYYY-MM-DD') between '{}' and '{}';"""

    del_shop_stat_sql = """ delete from public.dwd_shop_stat_by_day where mid = {} and to_char(stat_date,'YYYY-MM-DD') between '{}' and '{}'; """

    sql = shop_stat_sql.format(mid,mid,mid,beg_date,end_date)
    # database = db.database('database2')
    # conn = database.connect()
    # data = database.select(sql, conn)
    # database.close(conn)
    data = common.select_sql(sql)
    if data is None or len(data) == 0 :
        logger.warning("No data.")
        return -1

    del_sql = del_shop_stat_sql.format(mid,beg_date,end_date)
    common.execute_sql(del_sql)
    logger.info('mid={} {} 至 {} 数据已删除'.format(mid,beg_date,end_date))

    data = data.fillna(0)
    if common.insert_to_DB_from_DF(data, 'public.dwd_shop_stat_by_day', 1000) == 1:
        logger.error('插入 public.dwd_shop_stat_by_day 数据失败，mid= {}'.format(mid))

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
    logger.info('dwd_shop_stat_by_day starting ...')
    if args.data_date == '1':
        begin_date = (datetime.date.today() + datetime.timedelta(days=-1)).strftime("%Y-%m-%d")
        end_date = begin_date
        for mid in mall_id:
            shop_stat(begin_date, end_date, mid)  # 下载前日数据
    elif args.data_date == '2':
        if args.begin_date and args.end_date and args.begin_date <= args.end_date:
            for mid in mall_id:
                shop_stat(args.begin_date, args.end_date, mid)  # 批量下载多日数据
        else:
            logger.error('开始日期[{}] 大于 结束日期[{}]，请重新输入日期范围！'.format(args.begin_date,args.end_date))
            parser.print_help()
            exit(1)
    else:
        parser.print_help()
        exit(1)

    logger.info("插入 public.dwd_shop_stat_by_day duration: {0}".format(datetime.datetime.now() - start_time))
