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
import json
import numpy as np


def sales_contribution_stat(mid):
    # 销售贡献：销售额、客单价、销售坪效、各级别会员消费
    # 租金贡献：租金坪效
    # 集客力：拉新人数、用户消费、支付人数
    # 服务力：评价得分、投诉意见
    # 退货得分：退货订单数量

    shop_sales = """ 
WITH  shop_prm as ( SELECT c.id AS sid,c.mid,f.prm_category_1, f.prm_category_2, d.brand_name, d.area, g.paid_acc, d.beg_date, d.end_date, h.complaint, round(avg(ci.comment_score),2) comment_score, (date_part('day', CURRENT_DATE::TIMESTAMP - min(d.beg_date)::TIMESTAMP ) + 1.00 )::numeric(10,2) online_days  , (date_part('day', max(d.end_date)::TIMESTAMP - min(d.beg_date)::TIMESTAMP ) + 1.00)::numeric(10,2) contract_days     
                    FROM ods_shop c
                    LEFT JOIN ods_contract d ON c.ext_id::text = d.ext_id::text
                    LEFT JOIN dwd_shop_prm_category f ON c.id = f.sid
                    LEFT JOIN ( SELECT contract_no, sum(paid_acc) AS paid_acc
                                FROM ods_contract_fee_plan
                                WHERE  fee_type in ( 10001 ,10018) AND (status = ANY (ARRAY[0, 4, 5]))
                                GROUP BY contract_no) g ON d.contract_no::text = g.contract_no::text
                    LEFT JOIN (SELECT contract_no,count(*)  complaint FROM "ods_complaint_info" WHERE contract_no<>''  GROUP BY contract_no) h ON h.contract_no = d.contract_no
                    LEFT JOIN ods_comment_info ci ON ci.sid=c.id
                    WHERE  d.status = 23 AND c.mid = {}
                    GROUP BY c.id, c.mid, f.prm_category_1, f.prm_category_2, d.brand_name, d.area, g.paid_acc, d.beg_date,	d.end_date,	h.complaint

                    UNION

                    SELECT n.id AS sid, n.mid, s.prm_category_1, s.prm_category_2, m.brand_name, m.area, k.paid_acc, m.beg_date, m.end_date, l.complaint, round(avg(ct.comment_score),2) comment_score, (date_part('day', CURRENT_DATE::TIMESTAMP - min(m.beg_date)::TIMESTAMP ) + 1.00 )::numeric(10,2) online_days ,(date_part('day', max(m.end_date)::TIMESTAMP - min(m.beg_date)::TIMESTAMP ) + 1.00)::numeric(10,2) contract_days
                    FROM ods_shop n
                    LEFT JOIN ods_ly_contract m ON n.ext_id::text = m.ext_id::text
                    LEFT JOIN dwd_shop_prm_category s ON n.id = s.sid
                    LEFT JOIN ( SELECT contract_no,sum(paid_acc) AS paid_acc
                                FROM ods_contract_fee_plan
                                WHERE fee_type in ( 10001 ,10018) AND (status = ANY (ARRAY[0, 4, 5]))
                                GROUP BY contract_no) k ON m.contract_no::text = k.contract_no::text
                    LEFT JOIN (SELECT contract_no,count(*)  complaint FROM "ods_complaint_info" WHERE contract_no<>''  GROUP BY contract_no) l ON l.contract_no = m.contract_no
                    LEFT JOIN ods_comment_info ct ON ct.sid=n.id
                    WHERE m.status = 23 AND n.mid = {} AND m.contract_no not like '%modify%'
                    GROUP BY n.id ,    n.mid,    s.prm_category_1, s.prm_category_2,    m.brand_name,    m.area,    k.paid_acc,		m.beg_date,		m.end_date,		l.complaint
                    )

                    SELECT
                        a.mid,
                        a.sid,
                        b.prm_category_1,
                        b.prm_category_2,
                        round(SUM ( a.sales_amount ) / b.online_days,2) sales,
                        (case WHEN SUM ( a.pay_person ) <>0 THEN round( SUM ( a.sales_amount ) / SUM ( a.pay_person ), 2 ) ELSE 0 END) kedanjia ,
                        (case when b.area <>0 AND b.online_days<>0 THEN  round(SUM ( a.sales_amount ) / b.area / b.contract_days ,2) ELSE 0 END) as sales_efficiency,
                        (case WHEN b.area<>0 AND b.contract_days<>0 THEN round(sum(b.paid_acc) / b.area / b.contract_days,2) ELSE 0 END) as sales_area,
                        sum(a.refund_order)::numeric(10,2) / h.order_num::numeric(10,2) refund,
                        round(e.pay_people / b.online_days ,2 ) pay_person,
                        round(f.new_people / b.online_days ,2 ) new_person,
                        round(f.new_pay_amount / b.online_days ,2) new_person_pay,
                        (CASE 	WHEN b.complaint is NULL THEN 0	ELSE b.complaint END ) as complaint,
                        (CASE 	WHEN b.comment_score is NULL THEN 5.00	ELSE b.comment_score END ) as comment,
                        round(g.vip1 / b.online_days ,2 ) vip1,
                        round(g.vip2 / b.online_days ,2 ) vip2,
                        round(g.vip3 / b.online_days ,2 ) vip3
                    FROM
                        "dwd_shop_stat_by_day" a 
                        LEFT JOIN shop_prm b  ON 	a.sid=b.sid
                        LEFT JOIN public.ods_shop c ON a.sid=c.id 
                        LEFT JOIN (SELECT sid,count(*) pay_people FROM mall{}.dwa_user_visit_shop_stat GROUP BY sid) e ON a.sid=e.sid
                        LEFT JOIN (SELECT a.sid,count(a.gid) new_people,sum(a.pay_amount_total) new_pay_amount FROM(
                                            SELECT row_number() OVER (PARTITION BY sid, gid ORDER BY stat_date ASC) AS row_num,sid,gid,pay_amount_total FROM 
                                            mall{}.dwa_user_visit_shop_stat) a WHERE a.row_num = 1 GROUP BY a.sid) f on a.sid=f.sid
                        LEFT JOIN (SELECT mid,sid,
                                    sum(CASE 	WHEN grade_code = 1 THEN	pay_amount_total ELSE	0  END ) vip1,
                                    sum(CASE 	WHEN grade_code = 2 THEN	pay_amount_total ELSE	0  END ) vip2,
                                    sum(CASE 	WHEN grade_code = 3 THEN	pay_amount_total ELSE	0  END ) vip3
                                    FROM  (SELECT a.mid, A.sid,  (CASE  WHEN b.grade_code is NULL THEN  1 ELSE  b.grade_code  END)  as grade_code,  
                                            SUM ( pay_amount_total )  as pay_amount_total  FROM mall{}.dwa_user_visit_shop_stat A  LEFT JOIN public.dwd_user_info b ON A.gid = b.gid  GROUP BY a.mid,A.sid, (CASE  WHEN b.grade_code is NULL THEN  1 ELSE  b.grade_code  END) ) a GROUP BY mid,sid) g ON g.sid=c.id
                        LEFT JOIN ( SELECT mid,sid,sum(order_num) order_num FROM public.dwd_shop_stat_by_day WHERE mid = {} GROUP BY mid,sid ) h ON h.sid=a.sid
                    WHERE
                         a.mid = {} AND c.is_online = 't' and a.sid {} 
                    GROUP BY a.mid,a.sid,b.prm_category_1,b.prm_category_2,b.area,e.pay_people,f.new_people,f.new_pay_amount,b.complaint,b.comment_score,g.vip1,g.vip2,g.vip3,b.online_days,b.contract_days,h.order_num,b.paid_acc
                    HAVING b.area is NOT NULL 
                    ;"""

    exclude_sid = common.get_sys_var("environment variables", "special_shop")
    shop_sales_sql = shop_sales.format(mid, mid, mid, mid, mid, mid, mid, exclude_sid)

    # print(shop_sales_sql)

    shop_data = common.select_sql(shop_sales_sql)
    shop_data = shop_data.fillna(0)

    return shop_data


def prm_category(mid, category):
    sql = """SELECT prm_category_{}  FROM public.dwd_shop_prm_category WHERE mid={} GROUP BY mid,prm_category_{} having count(*) > 1; """
    category_sql = sql.format(category, mid, category)
    df = common.select_sql(category_sql)
    category_list = df['prm_category_' + str(category)].tolist()
    return category_list


def data_cut(df, colname, boxes, orderby):
    # 根据自定义数值范围分箱
    if orderby == 1:
        labels = [i + 1 for i in range(boxes)]
    elif orderby == 2:
        labels = [boxes - i for i in range(boxes)]
    else:
        logger.error('排序参数错误:{}，正序=1，倒序=2'.format(orderby))
    min = df[colname].min()
    max = df[colname].max()
    min = 0 if min is np.nan else min
    max = 0 if max is np.nan else max
    if min == max:
        if min == 0:
            max += 1
        else:
            min -= 1
    bins = list(np.linspace(min, max, num = boxes + 1, endpoint = True))
    bins[-1] += 1
    df[colname + '_score'] = pd.cut(df[colname], bins = bins, labels = labels, right = False).astype(float)


def data_qcut(df, colname, boxes):
    # 根据字段数值自动分箱
    labels = [i + 1 for i in range(boxes)]
    df[colname + '_score'] = pd.qcut(df[colname], boxes, precision = 2, labels = labels).astype(float)


def mall_brank_value(mid):
    logger.info('mid={},starting'.format(mid))
    shop_all = sales_contribution_stat(mid)
    for category_lvl in category_level:
        logger.info('mid={},category_level={},starting'.format(mid, category_lvl))
        for category in prm_category(mid, category_lvl):
            logger.info('mid={},category_level={},category={} starting'.format(mid, category_lvl, category))
            shop_data = shop_all[shop_all['prm_category_' + str(category_lvl)] == category].copy()
            data_cut(shop_data, 'sales', 10, 1)
            data_cut(shop_data, 'kedanjia', 2, 1)
            data_cut(shop_data, 'sales_efficiency', 8, 1)
            data_cut(shop_data, 'sales_area', 20, 1)
            data_cut(shop_data, 'pay_person', 15, 1)
            data_cut(shop_data, 'new_person', 10, 1)
            data_cut(shop_data, 'new_person_pay', 5, 1)
            data_cut(shop_data, 'pay_person', 15, 1)
            data_cut(shop_data, 'new_person', 10, 1)
            data_cut(shop_data, 'new_person_pay', 5, 1)
            data_cut(shop_data, 'complaint', 5, 2)
            data_cut(shop_data, 'comment', 5, 1)
            data_cut(shop_data, 'refund', 10, 2)
            data_cut(shop_data, 'vip1', 2, 1)
            data_cut(shop_data, 'vip2', 3, 1)
            data_cut(shop_data, 'vip3', 5, 1)
            shop_data['composite_score'] = shop_data.iloc[:, 17:].sum(axis = 1)
            shop_data['category_level'] = category_lvl
            shop_data['prm_category_id'] = category

            shop_data.drop(labels = ['prm_category_1', "prm_category_2"], axis = 1, inplace = True)
            delete_data = f""" DELETE FROM public.dwa_shop_value a WHERE a.mid ={mid} 
                        and a.prm_category_id = {category} and a.category_level={category_lvl}
                        AND EXISTS (SELECT 1 FROM public.ods_shop WHERE a.sid=id AND is_online = 't') ; """
            common.execute_sql(delete_data)
            if common.insert_to_DB_from_DF(shop_data, 'public.dwa_shop_value', 1000) == 1:
                logger.error('插入 public.dwa_shop_value 数据失败，mid= {}, category = {}'.format(mid, category))
    logger.info('mid={},数据生成完毕'.format(mid))


def test():  # 用于调试单个或多个mall
    mall_id = ['5']
    for i in mall_id:
        mall_brank_value(i)
    logger.info("Total duration of brand value:{0}".format(datetime.datetime.now() - start_time))
    sys.exit(0)


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    work_dir, connect_db, postgres_host, postgres_port, postgres_user, postgres_password, postgres_datebase = common.get_config()
    mall_id = common.get_mallID()
    mall_id = [i for i in mall_id if i != '6' and i != '18']
    category_level = [1, 2]
    logger = common.bigdata_logger('analyse.log')

    # test()

    po = Pool(5)  # 定义个进程池
    for i in mall_id:
        po.apply_async(mall_brank_value, (i,), error_callback=common.pool_call_back)
        time.sleep(1)
    po.close()
    po.join()

    logger.info("Brand value Total duration {}".format(datetime.datetime.now() - start_time))
