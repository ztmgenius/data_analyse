# coding=utf-8
"""
Created on 2022-09-19
@author: Lin lefeng
执行方法：python3 dorris_import.py           # 单进程运行
        python3 dorris_import.py -p10      # 10个进程同时运行

update udf_import_conf set status=2 where 1=1

update udf_import_conf set status=1 where id=7

delete from umall_ods.udf_import_conf  where id=3

truncate table umall_ods.order_info_2

select count(*) from umall_ods.order_info_2 where to_date(date_add(ctime,INTERVAL 1 DAY))= CURDATE();
select count(*) from umall_ods.order_info_3;

alter table umall_ods.udf_import_conf add column history int NULL comment "是否补历史数据"
alter table umall_ods.udf_improt_conf modify column history int NULL comment "是否补历史数据"
alter table wx_work.conf_import add column src_isnull smallint default '1' comment "数据源是否允许无记录,1允许，0不允许"
ALTER TABLE wx_work.conf_import DROP COLUMN src_isnull;
alter table umall_ods.conf_import add column obj_unique_key text NULL AFTER obj_where;
alter table umall_ods.conf_import MODIFY COLUMN obj_unique_key COMMENT "主键列，多列逗号分隔";


select CURDATE(),date_add(CURDATE(),INTERVAL -1 DAY)
select * from dwd_category_interval_umall where trade_date=CURRENT_DATE-1
"""

import datetime
from multiprocessing import Pool
import pandas as pd
import time

import configure_file
import doris
import db
import public

conf_db = "umall_ods"
conf_table = "conf_import"


def verify_validity(src, obj):
    """
    检查作业配置参数合法性
    """
    if (len(src['where']) == 0 and len(obj['where']) != 0) or (len(src['where']) != 0 and len(obj['where']) == 0):
        logger.error(f"源表与目标表校验条件不一致:\tid: {src['id']}\tsrc: {src['where']}\tobj: {obj['where']}")
        return False

    if len(src['column']) == 0 and len(obj['column']) == 0:
        logger.error(f"源表或目标表列名不能为空:\tid: {src['id']}\nsrc: {src['column']}\nobj: {obj['column']}")
        return False

    if len(src['column']) != len(obj['column']):
        logger.error(f"源表或目标表列数量不一致:\tid: {src['id']}\tsrc: {len(src['column'])}个\tobj: {len(obj['column'])}个")
        return False

    src_col = [x[x.lower().rfind(' as ') + 4:] if x.lower().rfind(" as ") > 0 else x for x in src['column']]
    src_col = [x.split('.')[-1] for x in src_col]
    for i in range(len(src_col)):
        if src_col[i].lower() != obj['column'][i].lower():
            logger.error(f"第{i}列名称不匹配：{src['id']} {src['database']}.{src['schema']}.{src['table']} {src_col[i].lower()} != {obj['column'][i].lower()}")
            return False
    return True


def get_jobs_for_history():
    """
    读取导入历史数据作业列表
    :return:
    """
    # 以下从数据库配置读取：一次性、昨日、今日未成功
    data = doris.Doris(conf_db).select(f"""
            select * from {conf_db}.{conf_table} 
            where inuse=1 and history=1 and ( status>0 or finish_time is null )
            order by obj_operator 
    """)
    if data is None or data.empty:
        return
    jobs = []
    for index, row in data.iterrows():
        src = configure_file.configure_file().get_items(row['src_server'])
        src['src_server'] = row['src_server']
        src['src_dbtype'] = row['src_dbtype']
        src['database'] = row['src_db']
        src['schema'] = row['src_schema']
        src['table'] = row['src_table']
        src['column'] = [x.strip() for x in row['src_column'].split(',')]
        src['where'] = row['src_where']
        src['id'] = row['id']
        src['split_col'] = row['split_col']
        src['src_isnull'] = row['src_isnull']

        obj = configure_file.configure_file().get_items("doris")
        obj['database'] = row['obj_db']
        obj['table'] = row['obj_table']
        obj['column'] = [x.strip() for x in row['obj_column'].split(',')]
        obj['where'] = row['obj_where']
        obj['operator'] = row['obj_operator']

        if not verify_validity(src,obj):
            continue
        jobs.append([src, obj])
    return jobs


def get_jobs():
    """
    读取按天导入数据作业列表
    :return:
    """
    # 以下从数据库配置读取：一次性、昨日、今日未成功
    data = doris.Doris(conf_db).select(f"""
            select * from {conf_db}.{conf_table} 
            where inuse=1 and (history=0 or history is null) and
                ( ((DATE_FORMAT(finish_time,'%Y-%m-%d') < DATE_FORMAT(NOW(),'%Y-%m-%d') or status>0 or finish_time is null) and cycle=2 ) or (status>0 and cycle=1) )
            order by obj_operator
    """)
    if data is None or data.empty:
        return
    jobs = []
    for index, row in data.iterrows():
        src = configure_file.configure_file().get_items(row['src_server'])
        src['src_server'] = row['src_server']
        src['src_dbtype'] = row['src_dbtype']
        src['database'] = row['src_db']
        src['schema'] = row['src_schema']
        src['table'] = row['src_table']
        src['column'] = [x.strip() for x in row['src_column'].split(',')]
        src['where'] = row['src_where']
        src['id'] = row['id']
        src['split_col'] = row['split_col']
        src['src_isnull'] = row['src_isnull']

        obj = configure_file.configure_file().get_items("doris")
        obj['database'] = row['obj_db']
        obj['table'] = row['obj_table']
        obj['column'] = [x.strip() for x in row['obj_column'].split(',')]
        obj['where'] = row['obj_where']
        obj['operator'] = row['obj_operator']

        if not verify_validity(src, obj):
            continue
        jobs.append([src, obj])
    # logger.info(jobs)
    return jobs


def importing(src, obj, update=True):
    """
    导入数据并修改数据库状态
    :param src:源数据库，字典
    :param obj:目的数据库，字典
    :param update:是否更新数据库状态，多进程执行切片数据时，不更新
    :return:
    """
    logger.debug(f"importing : {src['id']}, {src['database']}.{src['schema']}.{src['table']}, {src['where']}")
    ret = doris.Doris(obj['database']).import_data(src, obj)
    if not ret:
        msg = f"导入失败：{src['database']}.{src['schema']}.{src['table']} -> {obj['database']}.{obj['table']}"
        # logger.error(msg)
        # public.send_notice_to_admin(11, msg)
    if not update:
        return
    # update log
    ftime = datetime.datetime.now()
    status = 0 if ret else 1
    ret = doris.Doris(conf_db).execute(f"update {conf_db}.{conf_table} set finish_time='{ftime}', status={status} where id={src['id']}")
    if ret:
        msg = f"修改日志出错：update {conf_db}.{conf_table} set finish_time='{ftime}', status={status} where id={src['id']}"
        logger.error(msg)
        # public.send_notice_to_admin(11, msg)


@public.running_duration
def import_from_pg():
    """
    循环作业列表，单进程执行导入数据
    :return:
    """
    jobs = get_jobs()
    if jobs is None or len(jobs) == 0:
        logger.warning(f"No importing data by day. ")
        return
    for job in jobs:
        src = job[0]
        obj = job[1]
        importing(src, obj)
    return


# @public.running_duration
def import2_from_pg(pools):
    """
    循环作业列表，多进程执行导入数据
    :param pools:进程数
    :return:
    """
    jobs = get_jobs()
    if jobs is None or len(jobs) == 0:
        logger.warning(f"No importing jobs by day. ")
        return

    pools = min(pools, len(jobs))
    po = Pool(pools)  # 定义个进程池
    for job in jobs:
        src = job[0]
        obj = job[1]
        # print(i, src['id'], src['where'])
        po.apply_async(importing, (src, obj), error_callback=public.error_call_back)
        time.sleep(3)
    po.close()
    po.join()
    return


# @public.running_duration
def import_history_data(pools, slice):
    """
    循环作业列表，多进程执行导入数据
    :param pools:进程数
    :return:
    """
    import copy
    jobs = get_jobs_for_history()
    if jobs is None or len(jobs) == 0:
        logger.warning(f"No importing history data. ")
        return

    for job in jobs:
        start_time = datetime.datetime.now()
        src = job[0]
        obj = job[1]
        obj['operator'] = 'append'
        src_where = src['where']
        obj_where = obj['where']
        split_col = src['split_col']  # 切片数据列名，可在配置表中定义
        if doris.Doris(obj['database']).execute(f"truncate table {obj['database']}.{obj['table']} "):
            logger.error(f"无法清空历史数据，truncate table {obj['database']}.{obj['table']} ")
            return

        # 数据切片
        sql = f"""select min(to_char({split_col},'yyyy-mm-dd')) as start_date,max(to_char({split_col},'yyyy-mm-dd')) as end_date ,count(*) as records
                    from {src['schema']}.{src['table']} where {src['where']}"""
        data = db.database(src['src_server'], db_name=src['database']).select(sql)
        if data is None or data.empty or data['records'][0] == 0:
            logger.info(f"数据库源表无数据:{src['src_server']}.{src['database']}.{src['table']}")
            continue
        if data['records'][0] < slice * 2:  # 小于500万不分片
            importing(src, obj)
            continue
        start_date = data['start_date'][0]
        end_date = public.date_add(data['end_date'][0], 1)
        days = public.date_minus(data['start_date'][0], data['end_date'][0])
        days = 1 if days == 0 else days
        freq = str(int(slice // (data['records'][0] // days)) + 1) + 'D'
        dates = pd.date_range(start=start_date, end=end_date, freq=freq)  # 分片天数间隔
        dates = [x.strftime('%Y-%m-%d') for x in dates]
        if dates[-1] < end_date:
            dates += [end_date]

        i = 0
        d_len = len(dates)
        logger.info(f"{src['database']}.{src['schema']}.{src['table']}共{round(data['records'][0]/10000,2)}万条记录，{d_len-1}个切片：\n{dates}")
        po = Pool(pools)  # 定义个进程池
        while i < d_len:
            start = dates[i]
            if i + 1 == d_len:
                end = start
            else:
                end = dates[i + 1]
            src['where'] = f"{split_col} >= '{start}' and {split_col}< '{end}' "
            obj['where'] = f"{split_col} >= '{start}' and {split_col}< '{end}' "
            logger.debug(f"正在导入切片数据：{i}, {start} -> {end}")
            src_copy = copy.deepcopy(src)
            obj_copy = copy.deepcopy(obj)
            po.apply_async(importing, (src_copy, obj_copy, False), error_callback=public.error_call_back)
            time.sleep(3)
            i += 1
            if i + 1 == d_len:
                break
        po.close()
        po.join()

        src['where'] = src_where
        obj['where'] = obj_where
        # 验证总记录数
        dbase = db.database(src['src_server'], db_name=src['database'])
        if 'where' in src.keys() and src['where'] is not None and len(src['where'].strip()) > 0:
            src_where = f" where {src['where']}"
        else:
            src_where = ''
        sql = f"select count(1) as count from {src['schema']}.{src['table']}{src_where} "
        data = dbase.select(sql)
        src_count = data['count'][0]

        if 'where' in obj.keys() and obj['where'] is not None and len(obj['where'].strip()) > 0:
            obj_where = f" where {obj['where']}"
        else:
            obj_where = ''
        sql = f"select count(1) as count from {obj['database']}.{obj['table']}{obj_where}"
        data = doris.Doris(obj['database']).select(sql)
        obj_count = data['count'][0]

        if src_count != obj_count:
            logger.error(f"{src['id']}\t{src['database']}.{src['schema']}.{src['table']},\t源数据{src_count}条，导入{obj_count}条\t条件:{src['where']}\t{obj['where']}")
            doris.Doris(conf_db).execute(f"update {conf_db}.{conf_table} set finish_time='{datetime.datetime.now()}', status=1 where id={src['id']}")
            return False
        else:
            doris.Doris(conf_db).execute(f"update {conf_db}.{conf_table} set finish_time='{datetime.datetime.now()}', status=0 where id={src['id']}")
            logger.info(f"{src['id']}\t{src['database']}.{src['schema']}.{src['table']}耗时：{datetime.datetime.now()-start_time}\t总记录数：{src_count}")
    return


def create_table_from_pg(create_table=False):
    # server, database, schema, table
    # conf_db = 'wx_work'  # 目标库
    table = [  # 定义需要导入的表
        ["bigdata_db", "postgres", "wx_work", [
                "dwa_groupowner_active_stat",
                "dwa_customer_by_month",
                "fraud_order_reference",
                "ods_shop_qw_owner",
                "ods_groupchat",
                "ods_user_behavior",
                "fraud_trade_reply",
                "fraud_push_config",
                "ods_department",
                "ods_groupchat_info",
                "fraud_trade",
                # "fraud_order_info",
                "cust_corp_tag",
                "cust_birthday_tag",
                "fraud_user_info",
                "ods_groupchat_stat_by_day",
                "dwd_user_add_crop_stat",
                "cust_tag",
                "user_task",
                "dwa_dept_stat_by_day",
                "fraud_user",
                "dwa_mall_stat_by_day",
                "promotion_info",
                "beike_ershoufang",
                "beike_loupan",
                "dwd_groupchat_stat",
                "dwd_user_stat",
                "fraud_parameter",
                "notice_receiver",
                "dwd_mall_dept",
                "ods_user_customer",
                # "ods_order_info",
                "invite_friend",
                "ods_groupchat_customer",
                "ods_owner_log",
                "dwd_mall_department",
                "dwd_customer_stat",
                "ods_department_user",
                "ods_user_customer_log"
            ]
        ]
    ]
    # t_lsit = [
    #             "dwa_fraud_detection",
    #             "dwa_holiday_pay_stat",
    #             "dwa_mall_rfm_stat",
    #             "dwa_prmcategory_user_stat",
    #             "dwa_user_consume_by_1month",
    #             "dwa_user_consume_by_3month",
    #             "dwa_user_consume_by_6month",
    #             "dwa_user_consume_by_7day",
    #             "dwa_user_consume_by_year",
    #             "dwa_user_pay_stat",
    #             "dwa_user_rfm",
    #             # "dwa_user_rfm_history",
    #             "dwa_user_visit_category_prm",
    #             "dwa_user_visit_category_umall",
    #             "dwa_user_visit_holiday",
    #             "dwa_user_visit_hour",
    #             "dwa_user_visit_shop_stat",
    #             "dwd_category_interval_prm",
    #             "dwd_category_interval_umall",
    #             "dwd_order_with_category",
    #             "dwd_user_category_prm",
    #             "dwd_user_category_umall",
    #             "dwd_user_consume_by_day",
    #             "dwd_user_park_info",
    #             "dwd_user_refund_info",
    #             "dwd_user_shop_interval",
    #             "dwd_user_trade_date"
    # ]
    # table = [  # 定义需要导入的表
    #     ["bigdata_db", "postgres", "mall1", t_lsit],
    #     ["bigdata_db", "postgres", "mall2", t_lsit],
    #     ["bigdata_db", "postgres", "mall3", t_lsit],
    #     ["bigdata_db", "postgres", "mall5", t_lsit],
    #     ["bigdata_db", "postgres", "mall6", t_lsit],
    #     ["bigdata_db", "postgres", "mall7", t_lsit],
    #     ["bigdata_db", "postgres", "mall9", t_lsit],
    #     ["bigdata_db", "postgres", "mall11", t_lsit],
    #     ["bigdata_db", "postgres", "mall18", t_lsit],
    #     ["bigdata_db", "postgres", "mall19", t_lsit],
    #     ["bigdata_db", "postgres", "mall21", t_lsit]
    # ]
    # t_lsit = [
    #             "dwa_association_rule",
    #             "dwa_association_rule_graph",
    #             "dwa_consumption_level",
    #             "dwa_fraud_group",
    #             "dwa_shop_sale_graph",
    #             "dwa_shop_stat_avg",
    #             "dwa_shop_value",
    #             "dwa_shop_visit_hour_by_day",
    #             "dwa_user_interaction",
    #             "dwa_user_retention_graph",
    #             "dwd_promotion_ecard_pay",
    #             "dwd_promotion_provide_stat",
    #             "dwd_promotion_use_stat",
    #             "dwd_promtion_stat_by_gid",
    #             "dwd_shop_prm_category",
    #             "dwd_shop_sales",
    #             "dwd_shop_stat_by_day",
    #             "dwd_user_info"
    #         ]
    # table = [  # 定义需要导入的表
    #     ["bigdata_db", "postgres", "public", t_lsit]
    # ]
    # table = [  # 定义需要导入的表
    #     ["bigdata_db", "postgres", "wx_work", ["ods_contact_way"]]
    # ]
    for tab in table:  # 循环源数据库
        pg_db = db.database(tab[0], db_name=tab[1])
        for t in tab[3]:  # 循环所有表
            data = pg_db.desc_table(tab[2], t)
            if data is None or data.empty:
                logger.info(f"表不存在：{tab[0]}.{tab[1]}.{tab[2]}.{t}")
                continue
            obj_tab_name = t + '_' + tab[2][4:] if tab[2][0:4] == 'mall' and tab[0] == "bigdata_db" and tab[1] == "postgres" else t
            ddl = f"CREATE TABLE {conf_db}.{obj_tab_name} (\n"
            field = ''
            fields = ''
            for index, row in data.iterrows():  # 循环所有列，进行类型转换
                if row['data_type'] == "character varying":
                    field = f"`{row['column_name']}` varchar ({row['character_maximum_length'] *2})"
                    field1 = f"""{t}.{row['column_name']}, """
                elif row['data_type'] == "text":
                    field = f"`{row['column_name']}` text "
                    field1 = f"""{t}.{row['column_name']}, """
                elif row['data_type'] == "smallint":
                    field = f"`{row['column_name']}` smallint "
                    field1 = f"""{t}.{row['column_name']}, """
                elif row['data_type'] == "integer":
                    field = f"`{row['column_name']}` bigint "
                    field1 = f"""{t}.{row['column_name']}, """
                elif row['data_type'] == "bigint":
                    field = f"`{row['column_name']}` largeint "
                    field1 = f"""{t}.{row['column_name']}, """
                elif row['data_type'] == "numeric":
                    if row['numeric_precision'] == row['numeric_scale']:
                        field = f"`{row['column_name']}` decimal({18 if row['numeric_precision']>18 else row['numeric_precision']}+1, {row['numeric_scale']})"
                    else:
                        field = f"`{row['column_name']}` decimal({18 if row['numeric_precision']>18 else row['numeric_precision']}, {row['numeric_scale']})"
                    field1 = f"""{t}.{row['column_name']}, """
                elif row['data_type'] == "double precision":
                    if row['numeric_precision'] == row['numeric_scale']:
                        field = f"`{row['column_name']}` decimal({row['numeric_precision']}+1, {row['numeric_scale']})"
                    else:
                        field = f"`{row['column_name']}` decimal({row['numeric_precision']}, {row['numeric_scale']})"
                    field1 = f"""{t}.{row['column_name']}, """
                elif row['data_type'] == "timestamp with time zone":
                    field = f"`{row['column_name']}` datetime "
                    field1 = f"""{t}.{row['column_name']}, """
                elif row['data_type'] == "timestamp without time zone":
                    field = f"`{row['column_name']}` datetime "
                    field1 = f"""{t}.{row['column_name']}, """
                elif row['data_type'] == "date":
                    field = f"`{row['column_name']}` date "
                    field1 = f"""{t}.{row['column_name']}, """
                elif row['data_type'] == "ARRAY":
                    field = f"`{row['column_name']}` text "
                    field1 = f"""cast ({t}.{row['column_name']} as text) as {row['column_name']}, """
                elif row['data_type'] == "json":
                    field = f"`{row['column_name']}` text "
                    field1 = f"""cast ({t}.{row['column_name']} as text) as {row['column_name']}, """
                elif row['data_type'] == "boolean":
                    field = f"`{row['column_name']}` boolean"
                    field1 = f"""{t}.{row['column_name']}, """
                elif row['data_type'] == "bytea":  # 暂不转换 todo
                    continue
                else:
                    logger.error(f"\n未定义类型：{row['table_catalog']}.{row['table_schema']}.{row['table_name']}:{row['column_name']} {row['data_type']}\n")
                    return
                # 特殊列处理
                if row['column_name'] == 'parent_order_no' or row['column_name'] == 'order_no':
                    if row['column_name'] == 'parent_order_no':
                        field = "`parent_order_no` largeint "
                        fields += 'CAST (parent_order_no as varchar) as parent_order_no, '
                    elif row['column_name'] == 'order_no':
                        field = "`order_no` largeint "
                        fields += 'CAST (order_no as varchar) as order_no, '
                else:
                    fields += field1

                if row['is_nullable'] == 'YES':
                    field += " NULL "
                else:
                    field += " NOT NULL "
                if row['column_default'] is not None:
                    if row['column_default'].lower() == "now()":
                        field += " DEFAULT CURRENT_TIMESTAMP "
                    if "::" in row['column_default']:
                        if not "nextval" in row['column_default']:
                            field += f""" DEFAULT {row['column_default'].split(':')[0]}"""
                field += f""" COMMENT "{'' if row['description'] is None or row['description']=='None' else row['description']}" """
                ddl = f"{ddl}{field},\n"
            fields = fields[:len(fields)-2]
            ddl = ddl[:len(ddl)-2]
            # 生成建表语句
            # 主键从配置表读取
            unique_key = doris.Doris(conf_db).select(f"select obj_unique_key from {conf_db}.{conf_table} where obj_db='{conf_db}' and obj_table='{obj_tab_name}'")
            if unique_key is not None and not unique_key.empty:
                u_key = unique_key['obj_unique_key'][0]
                if u_key is not None and len(u_key) > 0:  # 有主键id
                    ddl = f"""{ddl}
                        ) ENGINE=OLAP
                        UNIQUE KEY({u_key})
                        COMMENT "{row['table_desc']}" 
                        DISTRIBUTED BY HASH({u_key}) BUCKETS 1
                    """
                else:  # 无主键id
                    ddl = f"""{ddl}
                        ) ENGINE=OLAP
                        duplicate KEY({data['column_name'][0]})
                        COMMENT "{row['table_desc']}" 
                        DISTRIBUTED BY HASH({data['column_name'][0]}) BUCKETS 1
                    """
            else:
                ddl = f"""{ddl}
                    ) ENGINE=OLAP
                    duplicate KEY({data['column_name'][0]})
                    COMMENT "{row['table_desc']}" 
                    DISTRIBUTED BY HASH({data['column_name'][0]}) BUCKETS 1
                """

                # logger.info(f"\n\n该表无ID字段主键，默认创建DUPLICATE KEY({data['column_name'][0]})，请手动修改key/distributed并重新执行以下sql：\n\n{ddl}\n")
            ddl = f"""{ddl}
                    PROPERTIES (
                    "replication_allocation" = "tag.location.default: 3",
                    "version_info" = "1",
                    "in_memory" = "false",
                    "storage_format" = "V2"
                    );
            """
            # print(data)
            if not create_table:  # 只显示建表语句
                print(ddl)
                pass
            else:
                doris.Doris(conf_db).execute(f"drop table {conf_db}.{obj_tab_name}")  # 删除旧表
                if doris.Doris(conf_db).execute(ddl) == 0:  # 执行建表语句
                    logger.info(f"成功创建表：{tab[0]}.{tab[1]}.{tab[2]}.{obj_tab_name}")
            # print(f"\n源表字段：{fields}\n目标表字段：{', '.join(data['column_name'].tolist())}\n")

            #  自动生成导数据作业
            # obj_col = ', '.join(data['column_name'].tolist())
            obj_col = [x[x.lower().rfind(' as ') + 4:] if x.lower().rfind(" as ") > 0 else x for x in fields.split(',')]
            obj_col = [x.split('.')[-1] for x in obj_col]
            obj_col = ', '.join(obj_col)
            sql = f"select * from {conf_db}.{conf_table} where obj_db='{conf_db}' and obj_table='{obj_tab_name}' "
            doris_db = doris.Doris(conf_db)
            df = doris_db.select(sql)
            if df is not None and not df.empty:
                id = df['id'][0]
                # 更新主要列，保留原始设置
                sql = f"""
                    update {conf_db}.{conf_table} set src_server='{tab[0]}', src_db='{tab[1]}', src_schema='{tab[2]}', 
                        src_table='{t}', src_column='{fields}', obj_column='{obj_col}'
                        where id ={id}
                """
            else:
                sql = f"select max(id) as id from {conf_db}.{conf_table} "
                df = doris_db.select(sql)
                id = 1 if df['id'][0] is None else df['id'][0] + 1
                sql = f"""
                    INSERT INTO {conf_db}.{conf_table}
                    (id, src_server, src_dbtype, src_db, src_schema, src_table, src_column, src_where, src_isnull, 
                    obj_db, obj_table, obj_column, obj_where, obj_unique_key, obj_operator, 
                    finish_time, status, cycle, inuse, remark,history)
                    VALUES({id}, '{tab[0]}', 'postgres', '{tab[1]}', '{tab[2]}', '{t}', '{fields}', '', 1,
                    '{conf_db}', '{obj_tab_name}', '{obj_col}', '', '', 'truncate', 
                    '{datetime.date.today()}', 1, 2, 1, 'temp',0);
                """
            # print(sql)
            doris_db.execute(sql)


if __name__ == "__main__":
    import argparse
    logger = public.bigdata_logger(f'doris_import.log')
    logger.info(f'doris_import starting ')
    parser = argparse.ArgumentParser(description='Postgres->Doris数据导入')
    parser.add_argument('-p', dest="multiprocess", type=int, required=False, help='并发进程数量')
    parser.add_argument('-d', dest="database", required=False, help='目标数据库')
    parser.add_argument('-s', dest="slice", type=int, required=False, help='下载历史数据时，每个切片的平均记录数，单位：百万条')
    args = parser.parse_args()
    pools = args.multiprocess
    starttime = datetime.datetime.now()
    if args.database is not None:
        conf_db = args.database
    if args.slice is not None:
        slice = args.slice * 100 * 10000
    else:
        slice = 200 * 10000
    if pools is None:
        import_from_pg()
    elif pools == 0:  # 生成作业数据、建表
        create_table_from_pg(True)
        pass
    else:
        import_history_data(pools, slice)
        import2_from_pg(pools)
    logger.info(f"Total duration for importing {conf_db} : {datetime.datetime.now() - starttime}")
