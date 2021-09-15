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


def sales_contribution_stat(mid,category_lvl,category):
    # 销售贡献：销售额、客单价、销售坪效、各级别会员消费
    # 租金贡献：租金坪效
    # 集客力：拉新人数、用户消费、支付人数
    # 服务力：评价得分、投诉意见
    # 退货得分：退货订单数量

    shop_sales = """ 
WITH  shop_prm as ( SELECT c.id AS sid,    c.mid,    f.prm_category_1, f.prm_category_2,    d.brand_name,    d.area,    g.paid_acc,		d.beg_date,		d.end_date,		h.complaint ,		round(avg(ci.comment_score),2) comment_score     
                    FROM ods_shop c
                    LEFT JOIN ods_contract d ON c.ext_id::text = d.ext_id::text
                    LEFT JOIN dwd_shop_prm_category f ON c.id = f.sid
                    LEFT JOIN ( SELECT ods_contract_fee_plan.contract_no, sum(ods_contract_fee_plan.paid_acc) AS paid_acc
                                FROM ods_contract_fee_plan
                                WHERE ods_contract_fee_plan.fee_type = 10001 AND (ods_contract_fee_plan.status = ANY (ARRAY[0, 4, 5]))
                                GROUP BY ods_contract_fee_plan.contract_no) g ON d.contract_no::text = g.contract_no::text
                    LEFT JOIN (SELECT contract_no,count(*)  complaint FROM "ods_complaint_info" WHERE contract_no<>''  GROUP BY contract_no) h ON h.contract_no = d.contract_no
                    LEFT JOIN ods_comment_info ci ON ci.sid=c.id
                    WHERE  d.status = 23 AND c.mid = {}
                    GROUP BY c.id ,    c.mid,    f.prm_category_1, f.prm_category_2,    d.brand_name,    d.area,    g.paid_acc,		d.beg_date,		d.end_date,		h.complaint
                    
                    UNION
                    
                    SELECT n.id AS sid,    n.mid,    s.prm_category_1, s.prm_category_2,    m.brand_name,    m.area,    k.paid_acc,		m.beg_date,		m.end_date,		l.complaint ,		round(avg(ct.comment_score),2) comment_score
                    FROM ods_shop n
                    LEFT JOIN ods_ly_contract m ON n.ext_id::text = m.ext_id::text
                    LEFT JOIN dwd_shop_prm_category s ON n.id = s.sid
                    LEFT JOIN ( SELECT ods_contract_fee_plan.contract_no,sum(ods_contract_fee_plan.paid_acc) AS paid_acc
                                FROM ods_contract_fee_plan
                                WHERE ods_contract_fee_plan.fee_type = 10001 AND (ods_contract_fee_plan.status = ANY (ARRAY[0, 4, 5]))
                                GROUP BY ods_contract_fee_plan.contract_no) k ON m.contract_no::text = k.contract_no::text
                    LEFT JOIN (SELECT contract_no,count(*)  complaint FROM "ods_complaint_info" WHERE contract_no<>''  GROUP BY contract_no) l ON l.contract_no = m.contract_no
                    LEFT JOIN ods_comment_info ct ON ct.sid=n.id
                    WHERE m.status = 23 AND n.mid = {}
                    GROUP BY n.id ,    n.mid,    s.prm_category_1, s.prm_category_2,    m.brand_name,    m.area,    k.paid_acc,		m.beg_date,		m.end_date,		l.complaint
                    )

                    SELECT
                        a.mid,
                        a.sid,
                        SUM ( a.sales_amount ) sales,
                        (case WHEN SUM ( a.pay_person ) <>0 THEN round( SUM ( a.sales_amount ) / SUM ( a.pay_person ), 2 ) ELSE 0 END) kedanjia ,
                        (case when b.area <>0 AND (date_part('day', CURRENT_DATE::TIMESTAMP - min(b.beg_date)::TIMESTAMP) + 1)::numeric(10,2)<>0 THEN  round(SUM ( a.sales_amount ) / b.area / (date_part('day', CURRENT_DATE::TIMESTAMP - min(b.beg_date)::TIMESTAMP) + 1)::numeric(10,2),2) ELSE 0 END) as sales_efficiency,
                        (case WHEN b.area<>0 AND date_part('day', max(b.end_date)::TIMESTAMP - min(b.beg_date)::TIMESTAMP)::numeric(10,2)<>0 THEN round(sum(b.paid_acc) / b.area / date_part('day', max(b.end_date)::TIMESTAMP - min(b.beg_date)::TIMESTAMP)::numeric(10,2),2) ELSE 0 END) as sales_area,
                        sum(a.refund_order) refund,
                        e.pay_people pay_person,
                        f.new_people new_person,
                        f.new_pay_amount new_person_pay,
												(CASE 	WHEN b.complaint is NULL THEN 0	ELSE b.complaint END ) as complaint,
												(CASE 	WHEN b.comment_score is NULL THEN 5.00	ELSE b.comment_score END ) as comment,
												g.vip1,
												g.vip2,
												g.vip3
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
                    WHERE
                         a.mid = {} AND c.is_online = 't' and a.sid {} 
                    GROUP BY a.mid,a.sid,b.prm_category_1,b.prm_category_2,b.area,e.pay_people,f.new_people,f.new_pay_amount,b.complaint,b.comment_score,g.vip1,g.vip2,g.vip3
                    HAVING b.area is NOT NULL and  b.prm_category_{} = {}
                    ;"""

    delete_sql = """ DELETE FROM public.dwa_shop_value  a  WHERE a.mid = {} and a.prm_category_id = {}  AND EXISTS (SELECT 1 FROM public.ods_shop WHERE a.sid=id AND is_online = 't') ; """

    delete_data = delete_sql.format(mid,category)
    common.execute_sql(delete_data)

    exclude_sid = common.get_sys_var("environment variables", "special_shop")
    shop_sales_sql = shop_sales.format(mid,mid,mid,mid,mid,mid,exclude_sid,category_lvl,category)

    # print(shop_sales_sql)

    shop_data = common.select_sql(shop_sales_sql)
    shop_data = shop_data.fillna(0)

    return shop_data

def prm_category(mid,category):
    sql = """SELECT prm_category_{}  FROM public.dwd_shop_prm_category WHERE mid={} GROUP BY mid,prm_category_{} having count(*) > 1; """
    category_sql =  sql.format(category,mid,category)
    df = common.select_sql(category_sql)
    category_list = df['prm_category_'+str(category)].tolist()
    return category_list


def data_cut_bak(df, colname, boxes, orderby):
    # 根据自定义数值范围分箱
    if orderby == 1 :
        labels = [i + 1 for i in range(boxes)]
    elif orderby == 2:
        labels = [boxes - i for i in range(boxes)]
    else:
        logger.error('排序参数错误，正序=1，倒序=2'.format(orderby))

    bins = list(np.linspace(df[colname].min(), df[colname].max(), num = boxes + 1, endpoint = True))
    print(bins)
    if df[colname].min() == df[colname].max() == 0:
        #判断数据均为0时，处理bins和labels范围
        bins =  list(range(boxes+1))
        bins[-1] += 1
        labels = list(reversed(labels))
        # df[colname + '_score'] = pd.cut(df[colname], bins = bins, labels = labels, right = False).astype(float)

    elif df[colname].min() == df[colname].max() and df[colname].max() != 0 :
        ##判断数据相同且不为0时，更改bins范围
        bins = list(range(boxes + 1))
        bins[-1] += 1
        # df[colname + '_score'] = pd.cut(df[colname], bins = bins, labels = labels, right = False).astype(float)

    else:
        bins[-1] += 1

    df[colname + '_score'] = pd.cut(df[colname], bins = bins, labels = labels, right = False).astype(float)


def data_cut(df, colname, boxes, orderby):
    # 根据自定义数值范围分箱
    if orderby == 1 :
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
    labels = [i+1 for i in range(boxes)]
    df[colname+'_score'] = pd.qcut(df[colname], boxes, precision=2, labels=labels).astype(float)


if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    work_dir, connect_db, postgres_host, postgres_port, postgres_user, postgres_password, postgres_datebase = common.get_config()
    mall_id = [3,5,7,9,11,19,21]
    mall_id = [5]
    category_level = [1,2]
    logger = common.bigdata_logger('analyse.log')

    for mid in mall_id:
        logger.info('mid={},starting'.format(mall_id))
        for category_lvl in category_level:
            logger.info('mid={},category_level={},starting'.format(mall_id, category_lvl))
            for category  in prm_category(mid,category_lvl):
                logger.info('mid={},category_level={},category={} starting'.format(mall_id, category_lvl, category))
                shop_data = sales_contribution_stat(mid,category_lvl, category)
                data_cut(shop_data, 'sales', 10,1)
                data_cut(shop_data, 'kedanjia', 2,1)
                data_cut(shop_data, 'sales_efficiency', 8,1)
                data_cut(shop_data, 'sales_area', 20,1)
                data_cut(shop_data, 'pay_person', 15, 1)
                data_cut(shop_data, 'new_person', 10, 1)
                data_cut(shop_data, 'new_person_pay', 5,1)
                data_cut(shop_data, 'pay_person', 15, 1)
                data_cut(shop_data, 'new_person', 10, 1)
                data_cut(shop_data, 'new_person_pay', 5, 1)
                data_cut(shop_data, 'complaint', 5, 2)
                data_cut(shop_data, 'comment', 5, 1)
                data_cut(shop_data, 'refund', 10, 2)
                data_cut(shop_data, 'vip1', 2, 1)
                data_cut(shop_data, 'vip2', 3, 1)
                data_cut(shop_data, 'vip3', 5, 1)
                shop_data['composite_score'] = shop_data.iloc[:, 15:].sum(axis = 1)
                shop_data['category_level'] = category_lvl
                shop_data['prm_category_id'] = category
                if common.insert_to_DB_from_DF(shop_data, 'public.dwa_shop_value', 1000) == 1:
                    logger.error('插入 public.dwa_shop_value 数据失败，mid= {}, category = {}'.format(mid,category))
        logger.info('mid={},数据生成完毕'.format(mall_id))
