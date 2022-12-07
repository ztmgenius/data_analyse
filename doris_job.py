# coding=utf-8
"""
Created on 2022-08-24
@author: Lin lefeng
python /data/datax/bin/datax.py  --jvm="-Xms3G -Xmx3G" /data/t_order_11.json

select sales_date,sum(count) from umall.dwd_order_info doi group by sales_date order by sales_date desc

select sales_date,count(*) from umall_original.order_info doi group by sales_date order by sales_date desc

delete from umall.dwd_order_info where sales_date > '2022-08-21'

delete from umall.dwd_order_gid where sales_date > '2022-08-21'

delete from umall.dwd_payment_info where sales_date > '2022-08-21'

truncate table umall.dwd_order_gid ;
truncate table umall.dwd_order_info ;
truncate table umall.dwd_payment_info  ;

"""

import pandas as pd
import datetime

import public
import public_umall
import doris
import db


def get_last_date(table, field):
    db = doris.Doris()
    last_date = db.select(f"select max({field}) as date from {table}")
    if last_date['date'][0] is not None:
        last_date = last_date['date'][0]
    else:
        last_date = '2000-01-01'
    return last_date


@public.running_duration
def import_data_mall(table_name, select, where=''):
    last_date = get_last_date(table_name, field)
    if where == '':
        where = f" where {field} < '{today}' and {field} > '{last_date}' "
    else:
        where += f" and {field} < '{today}' and {field} > '{last_date}' "
    db = doris.Doris()
    for m in mid:
        s = select.format(m)
        s = f"""insert into {table_name} {s} {where} ;"""
        e = db.execute(s)
        logger.info(f"mid = {m}\ttable_name = {table_name}\tstatus = {e}")


@public.running_duration
def import_qyweixin(sql, drs_db, drs_table):
    db_pg = db.database('bigdata_db')
    print('1', datetime.datetime.now())
    data = db_pg.select(f'{sql}')
    data.drop(['tasks', 'last-modification'], axis=1, inplace=True)
    print('2', datetime.datetime.now())
    db_doris = doris.Doris(drs_db)
    db_doris.copy_to_DB_from_DF(data, drs_table, index=True)
    print('3', datetime.datetime.now())


if __name__ == "__main__":
    pass
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    # pd.set_option('precision', 3)
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)

    import argparse
    parser = argparse.ArgumentParser(description='Doris作业')
    parser.add_argument('-m', dest="manager", help='作业')  #
    args = parser.parse_args()
    if args.manager is not None:  #
        pass

    logger = public.bigdata_logger('doris_job.log', "doris_job")
    logger.info('doris_job starting ')

    today = datetime.date.today()
    field = 'sales_date'
    mid = public_umall.Mall().mid

    # import_qyweixin("select * from wx_work.ods_user_customer", 'umall_original', 'ods_user_customer_copy1')
    # exit()

    sql = """select mid,gid,sales_date,status,order_type,biz_type,origin_amount,pay_amount,allowance,score_amount,ecard_amount,mtime,1 from umall_original.order_info_{0}"""
    import_data_mall('umall.dwd_order_gid', sql)

    sql = """select mid,sid,sales_date,status,order_type,biz_type,origin_amount,pay_amount,allowance,score_amount,ecard_amount,1 from umall_original.order_info_{0}"""
    import_data_mall('umall.dwd_order_info', sql)

    sql = """select mid,pay_type,sales_date,status,pay_amount,1 from umall_original.payment_info_{0}"""
    import_data_mall('umall.dwd_payment_info', sql)
