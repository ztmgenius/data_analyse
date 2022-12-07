# coding=utf-8
"""
Created on 2022-08-11
@author: Lin lefeng
python /data/datax/bin/datax.py  --jvm="-Xms3G -Xmx3G" /data/t_order_11.json

实例名称	    端口名称	            默认端口	通讯方向	                说明
BE	    be_port	                9060	FE --> BE	            BE 上 thrift server 的端口，用于接收来自 FE 的请求
BE	    webserver_port	        8040	BE <--> BE	            BE 上的 http server 的端口
BE	    heartbeat_service_port	9050	FE --> BE	            BE 上心跳服务端口（thrift），用于接收来自 FE 的心跳
BE	    brpc_port           	8060	FE <--> BE, BE <--> BE	BE 上的 brpc 端口，用于 BE 之间通讯
FE	    http_port	            8030	FE <--> FE，用户 <--> FE	FE 上的 http server 端口
FE	    rpc_port	            9020	BE --> FE, FE <--> FE	FE 上的 thrift server 端口，每个fe的配置需要保持一致
FE	    query_port	            9030	用户 <--> FE	            FE 上的 mysql server 端口
FE	    edit_log_port	        9010	FE <--> FE	            FE 上的 bdbje 之间通信用的端口
Broker	broker_ipc_port	        8000	FE --> Broker, BE --> Broker	Broker 上的 thrift server，用于接收请求

CREATE TABLE IF NOT EXISTS umall.dwd_order_info
(
  `mid` bigint(20) DEFAULT "0" COMMENT "",
  `sid` bigint(20) DEFAULT "0" COMMENT "",
  `sales_date` date NULL COMMENT "",
  `status` bigint(20) NULL COMMENT "",
  `order_type` bigint(20) NULL COMMENT "",
  `biz_type` bigint(20) NULL COMMENT "",
  `origin_amount` decimal(12, 2) SUM DEFAULT "0"  COMMENT "",
  `pay_amount` decimal(12, 2) SUM DEFAULT "0"  COMMENT "",
  `allowance` decimal(12, 2) SUM DEFAULT "0"  COMMENT "",
  `score_amount` decimal(12, 2) SUM DEFAULT "0"  COMMENT "",
  `ecard_amount` decimal(12, 2) SUM DEFAULT "0"  COMMENT "",
  `count` bigint(20) SUM DEFAULT "0"  COMMENT ""
)
AGGREGATE KEY(`mid`, `sid`, `sales_date`, `status`, `order_type`, `biz_type`)
DISTRIBUTED BY HASH(`sid`) BUCKETS 3
PROPERTIES (
"replication_allocation" = "tag.location.default: 3"
);

delete from umall.dwd_order_info where mid=5

select count(1) from umall.dwd_order_info where mid=5

insert into umall.dwd_order_info select mid,sid,sales_date,status,order_type,biz_type,origin_amount,pay_amount,allowance,score_amount,ecard_amount,1 from umall_original.order_info_2;

select sales_date,order_type ,sum(origin_amount),sum(count) from umall.dwd_order_info doi where status=2 and sales_date >= '2021-08-15' and sales_date >= '2021-12-15' group by order_type,sales_date order by sales_date ;
select sales_date,order_type ,sum(origin_amount),sum(count) from umall.dwd_order_info doi where status=2 and sales_date >= '2021-12-15' and sales_date <= '2021-12-15' group by order_type,sales_date order by sales_date ;
select mid,sales_date,sum(origin_amount),sum(count) from umall.dwd_order_info doi where status=2 and sales_date >= '2022-08-15' and sales_date <= '2022-08-15' group by sales_date,mid order by mid,sales_date ;
select sid,sales_date,sum(origin_amount),sum(count) from umall.dwd_order_info doi where status=2 and sales_date >= '2022-08-15' and sales_date <= '2022-08-15' and mid=2 group by sales_date,sid order by sid,sales_date ;

insert into umall.dwd_payment_info (mid,pay_type,sales_date,status,pay_amount) select mid,pay_type,sales_date,status,pay_amount from umall_original.payment_info_2;

select c.mname,a.*,b.* from
(select mid,sales_date,sum(origin_amount) 总金额,sum(count) 总笔数 from umall.dwd_order_info doi where status=2 and sales_date >= '2021-08-15' and sales_date <= '2022-08-15' group by sales_date,mid order by mid,sales_date) a,
(
    SELECT p.mid,p.sales_date,
    max(case p.pay_type when -1 then p.cnt else 0 end) 方式_1笔数,
    max(case p.pay_type when -1 then p.amount else 0 end) 方式_1金额,
    max(case p.pay_type when 0 then p.cnt else 0 end) 方式0笔数,
    max(case p.pay_type when 0 then p.amount else 0 end) 方式0金额,
    max(case p.pay_type when 1 then p.cnt else 0 end) 方式1笔数,
    max(case p.pay_type when 1 then p.amount else 0 end) 方式1金额,
    max(case p.pay_type when 2 then p.cnt else 0 end) 方式2笔数,
    max(case p.pay_type when 2 then p.amount else 0 end) 方式2金额,
    max(case p.pay_type when 3 then p.cnt else 0 end) 方式3笔数,
    max(case p.pay_type when 3 then p.amount else 0 end) 方式3金额,
    max(case p.pay_type when 4 then p.cnt else 0 end) 方式4笔数,
    max(case p.pay_type when 4 then p.amount else 0 end) 方式4金额,
    max(case p.pay_type when 5 then p.cnt else 0 end) 方式5笔数,
    max(case p.pay_type when 5 then p.amount else 0 end) 方式5金额,
    max(case p.pay_type when 6 then p.cnt else 0 end) 方式6笔数,
    max(case p.pay_type when 6 then p.amount else 0 end) 方式6金额,
    max(case p.pay_type when 7 then p.cnt else 0 end) 方式7笔数,
    max(case p.pay_type when 7 then p.amount else 0 end) 方式7金额,
    max(case p.pay_type when 9 then p.cnt else 0 end) 方式9笔数,
    max(case p.pay_type when 9 then p.amount else 0 end) 方式9金额,
    max(case p.pay_type when 10 then p.cnt else 0 end) 方式10笔数,
    max(case p.pay_type when 10 then p.amount else 0 end) 方式10金额,
    max(case p.pay_type when 11 then p.cnt else 0 end) 方式11笔数,
    max(case p.pay_type when 11 then p.amount else 0 end) 方式11金额,
    max(case p.pay_type when 12 then p.cnt else 0 end) 方式12笔数,
    max(case p.pay_type when 12 then p.amount else 0 end) 方式12金额,
    max(case p.pay_type when 13 then p.cnt else 0 end) 方式13笔数,
    max(case p.pay_type when 13 then p.amount else 0 end) 方式13金额
    FROM
        (select mid,sales_date,pay_type,sum(count) as cnt,sum(pay_amount) as amount from umall.dwd_payment_info dpi where status=1 and sales_date >= '2021-08-15' and sales_date <= '2022-08-15'
        group by mid,sales_date,pay_type order by mid,pay_type
        ) p
    GROUP  BY p.mid,p.sales_date
    order by p.mid,p.sales_date
) b
left join umall_original.mall c on a.mid=c.id
where a.mid = b.mid and a.sales_date=b.sales_date
order by a.mid,a.sales_date

"""

import pandas as pd
from retry import retry
import pymysql
import math
import time
import datetime
import os
import subprocess
import json
import requests

import configure_file
import public

delay = 1  # 重试间隔，秒
tries = 10  # 重试次数

logger = public.bigdata_logger('doris.log', 'doris_logger')


class Doris(object):  # Doris数据库类  第一个参数：数据库名称
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    # pd.set_option('precision', 3)
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)

    def __init__(self, db=None):
        self.items = configure_file.configure_file().get_items("doris")
        self.conn = None
        if db is not None:
            self.items['database'] = db

    def __enter__(self):
        pass
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # print(f'type:{exc_type}')
        # print(f'value:{exc_value}')
        # print(f'trace:{traceback}')
        self.close()
        return True

    @retry(Exception, delay=1)  # jitter=(1, 3))
    def connect(self):
        try:
            # 连接database
            conn = pymysql.connect(
                host=self.items['host'],
                port=int(self.items['port']),
                user=self.items['user'],
                password=self.items['password'],
                database=self.items['database'],
                charset=self.items['charset']
            )
        except Exception as e:
            logger.error(f'ERROR: Can not connect {self.host}:{self.port} {e}')
            raise ValueError(f'ERROR: Can not connect {self.host}:{self.port} {e}')
        self.conn = conn
        return conn

    def truncate(self, tablename):  # tablename需要加schema
        self.execute(f"TRUNCATE TABLE {tablename}")

    def commit(self):
        self.conn.commit()

    def close(self):
        if self.conn is not None:
            self.conn.close()

    def select(self, sql_cmd):  # 执行select/with语句
        """
        缺点：数据类型识别不好，数值型、字符型都识别为object，需自行转换
        :param sql_cmd:
        :return: None if an error occur else dataframe
        """
        sql_cmd = sql_cmd.strip()
        self.connect()
        try:
            # 得到一个可以执行SQL语句的光标对象
            cursor = self.conn.cursor()  # 执行完毕返回的结果集默认以元组显示
            # 得到一个可以执行SQL语句并且将结果作为字典返回的游标
            cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
            # 执行SQL语句
            cursor.execute(sql_cmd)
        except Exception as e:
            logger.error(f"ERROR: execute执行sql出错:{sql_cmd} , {e}")
            self.close()
            return None
        data = pd.DataFrame(cursor.fetchall())  # 无列名
        if not data.empty:
            col_name = []  # 创建一个空列表以存放列名
            for c in cursor.description:  # 类似 desc table_name返回结果
                col_name.append(c[0])  # 循环提取列名，并添加到col空列表
            data.columns = col_name
            # data = data.convert_dtypes()
        cursor.close()
        self.close()
        return data

    def select_sql(self, sql_cmd):  # 执行select/with语句
        """
        与select的区别： 针对不同数据场景谨慎使用
        1、数据库timestamptz类型字段，读取后不带时区需自行转换！！！
            方法： data['ctime'] = pd.to_datetime(data['ctime'])
                  data['ctime'] = data['ctime'].dt.tz_convert(tz='Asia/Shanghai')
        2、优点是：自动识别数据类型，但不稳定，自动转换数值行数据为float，但对长整型（如订单号）自动转换为float，有问题
        3、适用于数据量不是很大
        4、read_sql have some serious issues when reading from e.g postgresSQL, even if chunk_size is specified
        """
        sql_cmd = sql_cmd.strip()
        data = pd.DataFrame()
        self.connect()
        try:
            data = pd.read_sql(sql_cmd, self.conn)  # 有列名
        except Exception as e:
            logger.error(f"ERROR: select_sql执行出错:{sql_cmd} , {e}")
            data = None
        self.close()
        # if data is not None and not data.empty:
        #     data = data.convert_dtypes()
        return data

    def execute(self, sql_cmd):  # 执行insert、update、delete 语句
        sql_cmd = sql_cmd.strip()
        self.connect()
        cur = self.conn.cursor()
        try:
            cur.execute(sql_cmd)
        except Exception as e:
            logger.error(f"ERROR: execute_sql执行sql出错:{sql_cmd} , {e}")
            self.close()
            return 1
        self.commit()
        self.close()
        return 0

    def check_load(self):
        res = self.execute("show routine load;")
        if res == 0:
            return res
        err_list = []
        print(u"查询到DRP的routine load任务数为：%d" % len(res))
        for row in res:
            logger.info(f"任务ID：{row['Id']}  任务名：{row['Name']}  目标数据库：{row['DbName']}  目标表名：{row['TableName']}  任务状态：{row['State']}")
            if (row['State'] == "RUNNING"):
                continue
            else:
                row_mess = f"任务名称：{row['Name']} 目标表名：{row['TableName']}   状态变化原因：{row['ReasonOfStateChanged']}  错误日志URL：{row['ErrorLogUrls']}  补充信息：{row['OtherMsg']}\n "
                err_list.append(row_mess)
        if len(err_list) > 0:
            logger.info(f'检查到模式下的异常routine load有：\n')
            for index, mess in enumerate(err_list):
                logger.info(mess)
        return res

    '''
    批量插入 
    data：为dataframe数据，size：为每次提交的记录大小
    sql示例：  "insert into table(username,password,userid) values(%s,%s,%s)"
    '''
    def batchInsert(self, sql, data, size):
        data = data.where(data.notnull(), None)
        ret = 0
        try:
            connection = self.connect()
            cursor = connection.cursor()
        except Exception as e:
            logger.error(f"ERROR: batchInsert connection, {e}")
            return 1
        cycles = math.ceil(data.shape[0] / size)
        for i in range(cycles):
            val = data[i * size:(i + 1) * size].values.tolist()
            try:
                cursor.executemany(sql, val)
                connection.commit()
            except Exception as e:
                logger.error(f"ERROR: batchInsert, {e}\n{val}, {sql}")
                connection.rollback()
                ret = 1
        connection.close()
        return ret

    def insert_to_DB_from_DF(self, data, table_name, size):
        """
        批量插入数据
        data : a DataFrame
        table_name : table name
        size : records once
        """
        table_columns = str(list(data.columns.array))[1:-1].replace("'", "").replace("\"", "")
        table_values = '%s'
        for i in range(0, len(data.columns) - 1):
            table_values = table_values + ',' + '%s'
        sql = "insert into {}({}) values ({})".format(table_name, table_columns, table_values)
        if self.batchInsert(sql, data, size) == 1:
            logger.error(f"ERROR: insert_to_DB_from_DF ,{table_name}, {sql}")
            return 1
        return 0

    def save_to_DB_from_DF(self, data: pd.DataFrame, tablename):  # 把dataframe保存为数据库表
        """
        :param data: dataframe
        :param tablename: save to table name
        :param schema: schema of table
        :return: int , <>0 is failed
        若表不存在则直接创建(创建不适用于Doris)并追加数据，字段名称必须一致，顺序可以不一致
        比insert_to_DB_from_DF快一倍
        https://blog.csdn.net/xc_zhou/article/details/118829588
        """
        from sqlalchemy import create_engine
        if data is None or data.empty:
            logger.info(f"数据集为None或空，不进行插入操作")
        engine = f"mysql+pymysql://{self.items['user']}:{self.items['password']}@{self.items['host']}:{self.items['port']}/{self.items['database']}?charset={self.items['charset']}"
        try:
            engine = create_engine(engine, echo=False)
            data.to_sql(tablename, con=engine, if_exists="append", index=False)
        except Exception as e:
            logger.error(f"ERROR: to_sql执行出错:{e}")
            engine.dispose()  # 关闭engine
            return 1

        engine.dispose()  # 关闭engine
        return 0

    def copy_to_DB_from_DF(self, data, table_name, index=False, database=None):
        """
        :param data: dataframe
        :param table_name: table name
        :param index
        :param database
        :return:
        100w条数据入库时间对比：
            insert ... 0:06:11.323474
            save   ... 0:01:21.860766
            copy   ... 0:00:11.978357
        """
        if data is None or len(data) == 0:
            logger.warning("数据集为空，不处理")
            return
        if database is None:  # 取当前数据库
            if len(table_name.split('.')) > 1:
                database = table_name.split('.')[0]
                table_name = table_name.split('.')[1]
            else:
                database = self.items['database']

        file_name = configure_file.configure_file().get_item('environment variables',
                                                             'work_dir') + f'/temp_data_{table_name}.csv'
        data.to_csv(file_name, encoding='utf-8', index=index, sep=",", header=False)

        label = 'temp_' + table_name + time.strftime("%Y%m%d%H%M%S")
        # 导入新数据
        cmd = f"""curl --location-trusted -u {self.items['user']}:{self.items['password']} -T {file_name} -H "label:{label}" -H "column_separator:," -H "columns:{','.join(data.columns)}" -XPUT http://{self.items['host']}:{self.items['http_port']}/api/{database}/{table_name}/_stream_load"""
        returncode, out = self.execute_cmd(cmd)
        if returncode != 0:
            raise SystemExit('execute {0} err :{1}'.format(cmd, out))
        out = json.loads(str(out, 'utf-8'))
        """
        {'TxnId': 23041, 'Label': 'temp_test_linlefeng133230', 'TwoPhaseCommit': 'false', 'Status': 'Success', 'Message': 'OK', 'NumberTotalRows': 1001, 'NumberLoadedRows': 1001, 'NumberFilteredRows': 0, 'NumberUnselectedRows': 0, 'LoadBytes': 16584, 'LoadTimeMs': 141, 'BeginTxnTimeMs': 0, 'StreamLoadPutTimeMs': 1, 'ReadDataTimeMs': 0, 'WriteDataTimeMs': 120, 'CommitAndPublishTimeMs': 17}
        {'TxnId': -1, 'Label': "temp_test_linlefeng'2022-08-24 133437", 'TwoPhaseCommit': 'false', 'Status': 'Fail', 'Message': "errCode = 2, detailMessage = Label format error. regex: ^[-_A-Za-z0-9]{1,128}$, label: temp_test_linlefeng'2022-08-24 133437", 'NumberTotalRows': 0, 'NumberLoadedRows': 0, 'NumberFilteredRows': 0, 'NumberUnselectedRows': 0, 'LoadBytes': 0, 'LoadTimeMs': 0, 'BeginTxnTimeMs': 0, 'StreamLoadPutTimeMs': 0, 'ReadDataTimeMs': 0, 'WriteDataTimeMs': 0, 'CommitAndPublishTimeMs': 0}
        {'TxnId': 23118, 'Label': 'temp_test_linlefeng20220824133804', 'TwoPhaseCommit': 'false', 'Status': 'Fail', 'Message': 'too many filtered rows', 'NumberTotalRows': 1002, 'NumberLoadedRows': 1001, 'NumberFilteredRows': 1, 'NumberUnselectedRows': 0, 'LoadBytes': 16596, 'LoadTimeMs': 117, 'BeginTxnTimeMs': 2, 'StreamLoadPutTimeMs': 1, 'ReadDataTimeMs': 0, 'WriteDataTimeMs': 112, 'CommitAndPublishTimeMs': 0, 'ErrorURL': 'http://172.16.17.194:8040/api/_load_error_log?file=__shard_2/error_log_insert_stmt_3a417635d6b0f090-51dd95a2f996748b_3a417635d6b0f090_51dd95a2f996748b'}
        """
        if out['Status'] == 'Fail':
            logger.error(str(out))
        else:
            logger.info(str(out))
        os.remove(file_name)

    def export_to_dataframe(self, sql):
        """
        使用datax工具从Doris读取数据到python dataframe，适用于数据量较大的sql查询，百万级以上
        :return: dataframe
        """
        jdbcUrl = f"jdbc:mysql://{self.items['host']}:{self.items['port']}/"
        file_name = str(int(time.time() * 1000))
        json_dict = {
            "core": {
                "transport": {
                    "channel": {
                        "speed": {
                            "channel": 4,
                            "record": -1,
                            "byte": -1,
                            "batchSize": 2048
                        }
                    }
                }
            },
            "job": {
                "setting": {
                    "speed": {
                        "channel": 3
                    },
                    "errorLimit": {
                        "record": 0,
                        "percentage": 0
                    }
                },
                "content": [
                    {
                        "reader": {
                            "name": "mysqlreader",
                            "parameter": {
                                "username": self.items['user'],
                                "password": self.items['password'],
                                "connection": [{
                                    "querySql": [sql],
                                    "jdbcUrl": [jdbcUrl]
                                }]
                            }
                        },
                        "writer": {
                            "name": "txtfilewriter",
                            "parameter": {
                                "path": self.items['datax_json_path'],
                                "fileName": file_name+".csv",
                                "writeMode": "truncate",
                                "fieldDelimiter": ",",
                                "fileFormat":"csv"
                            }
                        }
                    }
                ]
            }
        }
        datax_json_path = f"""{self.items['datax_json_path']}/{file_name}.json"""
        json.dump(json_dict, open(datax_json_path, 'w'), indent=4)
        cmd =f"""{configure_file.configure_file().get_item('environment variables', 'python')} {self.items['datax']} --jvm="-Xms3G -Xmx3G" {datax_json_path}"""
        logger.debug(f"正在运行...\n\t{cmd}")
        returncode, out = self.execute_cmd(cmd)
        if returncode != 0:
            logger.error(f"执行导出数据出错，退出系统：{cmd}，{str(out, 'utf-8')}")
            return False

        files = os.listdir(self.items['datax_json_path'])
        for f in files:
            if f[:len(file_name)] == file_name and f != file_name+'.json':
                file_name = f"{self.items['datax_json_path']}/{f}"
                break
        data = pd.read_csv(file_name, index_col=False, header=None)
        os.remove(file_name)
        os.remove(datax_json_path)
        return data

    def import_data(self, source_db, object_db):
        """
        从pg导入到Doris
        :param source_db:
        :param object_db:
        :param object_db:delete, truncate, append
        :return:
        """
        start_time = datetime.datetime.now()

        import db
        dbase = db.database(source_db['src_server'], db_name=source_db['database'])
        if 'where' in source_db.keys() and source_db['where'] is not None and len(source_db['where'].strip()) > 0:
            src_where = f" where {source_db['where']}"
        else:
            src_where = ''
        sql = f"select count(1) as count from {source_db['schema']}.{source_db['table']}{src_where} "
        data = dbase.select(sql)
        if data is None or data.empty or data['count'][0] == 0:
            msg = f"无符合条件的数据需要导入，数据源： {source_db['database']}.{source_db['schema']}.{source_db['table']} {src_where}"
            if source_db['src_isnull'] == 0:
                logger.error(msg)
                return False
            else:
                logger.warning(msg)
                return True
        src_count = data['count'][0]

        src_obj = f"{source_db['id']}-{source_db['database']}.{source_db['schema']}.{source_db['table']} -> {object_db['database']}.{object_db['table']}"
        feLoadUrl = [x.strip() for x in self.items['fe_load_url'].split(',')]
        beLoadUrl = [x.strip() for x in self.items['be_load_url'].split(',')]
        jdbcUrl = f"jdbc:mysql://{self.items['host']}:{self.items['port']}/"
        if object_db['operator'].lower().strip() == 'truncate':
            sql = f"truncate table {object_db['database']}.{object_db['table']}"
            logger.info(sql)
            self.execute(sql)
        elif object_db['operator'].lower().strip() == 'delete':
            if 'where' in object_db.keys() and object_db['where'] is not None and len(object_db['where'].strip()) > 0:
                sql = f"delete from {object_db['database']}.{object_db['table']} where {object_db['where']} "
                logger.info(sql)
                if self.execute(sql):
                    return False
            else:
                logger.error(f"没有where条件无法执行delete操作：{object_db['database']}.{object_db['table']}")
                return False

        json_dict = {
            "core": {
                "transport": {
                    "channel": {
                        "speed": {
                            "channel": 4,
                            "record": -1,
                            "byte": -1,
                            "batchSize": 2048
                        }
                    }
                }
            },
            "job": {
                "setting": {
                    "speed": {
                        "channel": 1
                    },
                    "errorLimit": {
                        "record": 0,
                        "percentage": 0
                    }
                },
                "content": [
                    {
                        "reader": {
                            "name": "postgresqlreader",
                            "parameter": {
                                "where": source_db['where'],
                                "column": source_db['column'],
                                "encoding": "UTF-8",
                                "password": source_db['password'],
                                "readName": "false",
                                "username": source_db['user'],
                                "connection": [
                                    {
                                        "table": [source_db['schema']+"." + source_db['table']],
                                        "jdbcUrl": [f"jdbc:postgresql://{source_db['host']}:{source_db['port']}/{source_db['database']}"]
                                    }
                                ]
                            }
                        },
                        "writer": {
                            "name": "doriswriter",
                            "parameter": {
                                "feLoadUrl": feLoadUrl,
                                "beLoadUrl": beLoadUrl,
                                "jdbcUrl": jdbcUrl,
                                "database": object_db['database'],
                                "table": object_db['table'],
                                "column": object_db['column'],
                                "username": object_db['user'],
                                "password": object_db['password'],
                                "postSql": [],
                                "preSql": [],
                                "loadProps": {},
                                "maxBatchRows": 200000,
                                "maxBatchByteSize": 104857600,
                                "lineDelimiter": "\n"
                            }
                        }
                    }
                ]
            }
        }
        postfix = datetime.datetime.now()
        postfix = str(postfix.second*1000*1000+postfix.microsecond)
        datax_json_path = f"""{self.items['datax_json_path']}/{source_db['id']}-{source_db['database']}.{source_db['schema']}.{source_db['table']}.{time.strftime("%Y-%m-%d", time.localtime())}.{postfix}.json"""
        json.dump(json_dict, open(datax_json_path, 'w'), indent=4)
        cmd =f"""{configure_file.configure_file().get_item('environment variables', 'python')} {self.items['datax']} --jvm="-Xms3G -Xmx3G" {datax_json_path}"""
        logger.debug(f"正在运行...\n\t{cmd}")
        returncode, out = self.execute_cmd(cmd)
        if returncode != 0:
            logger.error(f"执行导出数据出错，退出系统：{cmd}，{str(out, 'utf-8')}")
            return False
        os.remove(datax_json_path)
        time.sleep(5)

        if 'where' in object_db.keys() and object_db['where'] is not None and len(object_db['where'].strip()) > 0:
            obj_where = f" where {object_db['where']}"
        else:
            obj_where = ''
        sql = f"select count(1) as count from {object_db['database']}.{object_db['table']}{obj_where}"
        data = self.select(sql)
        if data is None or data.empty or data['count'][0] == 0:
            logger.error(f"源表共{src_count}条，目标表未导入任何数据:{sql}")
            return False
        obj_count = data['count'][0]

        where = '全量' if source_db['where'] is None or len(source_db['where']) == 0 else source_db['where']
        if src_count != obj_count:
            msg = f"{src_obj},\t源数据{src_count}条，导入{obj_count}条\t条件: {where}"
            logger.error(msg)
            return False
        logger.info(f"成功导入数据: {src_obj}\t共{src_count}条\t用时：{datetime.datetime.now()-start_time}\t条件: {where}")
        return True

    def execute_cmd(self, cmd):  # 执行psql命令
        p = subprocess.Popen(cmd,
                             shell=True,
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()

        if p.returncode != 0:
            return p.returncode, stderr
        return p.returncode, stdout


def performance_testing():
    """
    数据导入性能测试：测试表
        CREATE TABLE `test_linlefeng` (
          `k1` largeint(40) NULL COMMENT "",
          `k2` decimal(10, 2) NULL DEFAULT "10.5" COMMENT "",
          `k3` char(10) NULL COMMENT "string column",
          `k4` int(11) NOT NULL DEFAULT "1" COMMENT "int column"
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`, `k2`)
        COMMENT "my first table"
        DISTRIBUTED BY HASH(`k1`) BUCKETS 32
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 3",
        "in_memory" = "false",
        "storage_format" = "V2"
        )
    :return:  函数    用时
        insert ... 0:04:35.782168
        save   ... 0:01:48.435503
        copy   ... 0:00:06.872522
    """
    db = Doris()
    k1 = []
    k2 = []
    k3 = []
    k4 = []
    for i in range(1000000):
        k1 += [i]
        k2 += [i]
        k3 += ['T'+str(i)]
        k4 += [i]
    dict1 = {
        'k1': k1,
        'k2': k2,
        'k3': k3,
        'k4': k4}
    data = pd.DataFrame(dict1)
    s = datetime.datetime.now()
    db.insert_to_DB_from_DF(data, 'test_linlefeng', 1000)
    print('insert ...', datetime.datetime.now() - s)
    s = datetime.datetime.now()
    db.save_to_DB_from_DF(data, 'test_linlefeng')
    print('save   ...', datetime.datetime.now() - s)
    s = datetime.datetime.now()
    db.copy_to_DB_from_DF(data, 'test_linlefeng')
    print('copy   ...', datetime.datetime.now() - s)


class DorisApi(object):  # Doris数据库类  第一个参数：数据库名称
    def __init__(self, db=None):  # mid 可以是整型或字符型，但必须是数字
        items = configure_file.configure_file().get_items("doris")
        self.user = items['user']
        self.password = items['password']
        self.fe_host = items['host']
        self.fe_http_port = items['http_port']
        self.url = f"http://{self.fe_host}:{self.fe_http_port}"
        self.headers = {  # http头
            "Content-Type": "application/json;charset=utf-8",
            "Authorization": f"Basic {self.get_authorization()}"
        }

    def __enter__(self):
        pass
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # print(f'type:{exc_type}')
        # print(f'value:{exc_value}')
        # print(f'trace:{traceback}')
        return True

    def get_authorization(self):
        """
        获得认证
        :return: 认证串
        """
        import base64
        auth = str(base64.b64encode(f"{self.user}:{self.password}".encode('utf-8')), 'utf-8')
        return auth

    def get_data(self, url, **kwargs):
        """
        :return:
        """
        try:
            resp = requests.get(url=url, headers=self.headers, timeout=5, **kwargs)
        except Exception as e:
            logger.warning(f"无法连接Doris: {url}, {e}")
            return
        if resp.status_code != 200:
            logger.warning(f"错误码：{resp.status_code}，{resp.text}")
            return
        try:
            resp = resp.json()
        except Exception as e:
            logger.warning(f"接口调用错误：{url}，{resp.text}")
            return
        if 'code' not in resp.keys() or resp['code'] != 0:
            logger.warning(f"{resp}")
            return
        # logger.info(f"{resp}")
        return resp

    def call_api(self, api_name: str):
        """
        api_name: 接口名称，如/api/backends
        :return:
        """
        url = self.url + api_name.strip()
        resp = self.get_data(url)
        if resp is None:
            return
        resp = resp['data']
        return resp

    @property
    def backends_action(self):
        """
        :return:
        """
        return self.call_api('/api/backends')['backends']

    @property
    def health(self):
        """
        :return:
        """
        return self.call_api('/api/health')

    @property
    def bootstrap(self):
        """
        :return:
        """
        return self.call_api('/api/bootstrap')

    @property
    def ha(self):
        """
        :return:
        """
        return self.call_api('/rest/v1/ha')

    @property
    def runtime_info(self):
        """
        :return:
        """
        return self.call_api('/api/show_runtime_info')

    def get_DDL_statement(self, db, table):
        """
        :return:
        """
        resp = self.call_api(f'/api/_get_ddl?db={db}&table={table}')
        for i in resp['create_partition']:
            print('create_partition:\n\n', i, '\n'+'-'*60+'\n')
        for i in resp['create_table']:
            print('create_table:\n\n', i, '\n'+'-'*60+'\n')
        for i in resp['create_rollup']:
            print('create_rollup:\n\n', i, '\n'+'-'*60+'\n')
        return resp


if __name__ == "__main__":
    pass
    # sql = "SHOW DATABASES;"
    # sql = "desc umall.test_linlefeng"
    # sql = "select * from order_info_5 ;"
    # s = datetime.datetime.now()
    # print(Doris('umall_original').select(sql))  # select_sql 0:01:28.384888
    # print('copy   ...', datetime.datetime.now() - s)
    # print(Doris().select(sql))
    # print(Doris().check_load())
    # performance_testing()
    # sql = f"""select * from test_linlefeng limit 10 INTO OUTFILE "file:///home/test/result_"; """
    # print(Doris().execute(sql))
    # data = Doris().export_to_dataframe("SELECT m.mname,s.* from umall_ods.shop s, umall_ods.mall m where s.mid = m.id ")
    # print(data)
    # df_all = pd.DataFrame({"id":[13,14],"a":[1665676800,1665676800],"b":[1665676800,1665676800]})
    # df_all['a'] = pd.to_datetime(df_all['a'], unit='s', utc=True)
    # df_all['b'] = pd.to_datetime(df_all['b'], unit='s', utc=True)
    # print(df_all)
    # # print(Doris().execute("""insert into wx_work.order_info_3 (id,a,b) VALUES (22,'2022-09-09','2022-09-09');"""))
    # if Doris('wx_work').insert_to_DB_from_DF(df_all, 'wx_work.order_info_3', 500) == 1:
    #     logger.info("「联系我」入库失败:get_contact_way()")
    # print(df_all)

    api = DorisApi()
    r = api.backends_action
    # r = api.get_DDL_statement('wx_work', 'conf_import1')
    # r = api.health
    # r = api.runtime_info
    # r = api.bootstrap
    # r = api.ha
    print(r)
