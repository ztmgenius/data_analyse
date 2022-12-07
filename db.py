"""
Created on 2021-07-30
@author: Lin lefeng

psql -h 172.16.17.200 -p 5432 -U postgres -d postgres -c "copy wx_work.user_task to '/home/test/user_task.copy' with binary"
psql -h 172.16.17.200 -p 5432 -U postgres -d postgres -c "copy test.user_task from '/home/test/user_task.copy' with binary"
"""
import os
import psycopg2
import pandas as pd
from dbutils.pooled_db import PooledDB
import math
import subprocess
import re
from retry import retry
import time
import datetime

import configure_file

delay = 1  # 重试间隔，秒
tries = 20  # 重试次数


def database_logger(log_filename='database.log', logger_name='database_logger'):
    import datetime
    import logging.handlers
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    log_file = configure_file.configure_file().get_item('environment variables', 'log_dir') + log_filename
    rf_handler = logging.handlers.TimedRotatingFileHandler(log_file, when='midnight', interval=1, backupCount=7, atTime=datetime.time(0, 0, 0, 0))
    rf_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s"))
    f_handler = logging.StreamHandler()  # 标准输出
    # f_handler = logging.FileHandler(work_dir + '/log/error.log', encoding='utf-8')   # 写入日志文件
    f_handler.setLevel(logging.INFO)
    f_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s"))
    logger.addHandler(rf_handler)
    logger.addHandler(f_handler)
    return logger


logger = database_logger('database.log', 'database_logger')
# logger.info('database log starting ')


class database(object):  # 数据库类  第一个参数：数据库节，第二个参数：mid，适用于按商场分库的连接
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    # pd.set_option('precision', 3)
    pd.set_option('display.unicode.ambiguous_as_wide', True)
    pd.set_option('display.unicode.east_asian_width', True)

    def __init__(self, db_server, mid='', db_name=''):  # mid 可以是整型或字符型，但必须是数字
        items = configure_file.configure_file().get_items(db_server)
        if db_name != '':
            items["database"] = db_name
        self.host = items["host"]
        self.port = items["port"]
        self.user = items["user"]
        self.password = items["password"]
        self.conn = None
        if type(mid) is str:
            mid = '' if mid == '0' or mid == '' else '_' + mid
        if type(mid) is int:
            mid = '' if mid == 0 else '_' + str(mid)
        self.database = items["database"] + mid
        self.psql_cmd = f"/usr/local/pgsql-11.0/bin/psql -h {self.host} -p {self.port} -U {self.user} -d {self.database} -c"

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
            conn = psycopg2.connect(host=self.host, port=self.port, user=self.user, password=self.password, database=self.database)
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
        cur = self.conn.cursor()
        if sql_cmd[0:5].lower() == 'call ':  # 执行存储过程
            self.conn.autocommit = True
        try:
            cur.execute(sql_cmd)
        except Exception as e:
            logger.error(f"ERROR: execute执行sql出错:{sql_cmd} , {e}")
            self.close()
            return None
        data = pd.DataFrame(cur.fetchall())  # 无列名
        if not data.empty:
            col_name = []  # 创建一个空列表以存放列名
            for c in cur.description:  # 类似 desc table_name返回结果
                col_name.append(c[0])  # 循环提取列名，并添加到col空列表
            data.columns = col_name
            # data = data.convert_dtypes()
        self.close()
        return data

    @retry(Exception, delay=delay, tries=tries)  # jitter=(1, 3))
    def __rselect__(self, sql_cmd):  # 执行select/with语句
        """
        带重试次数的select,调用该函数必须与try结合使用，否则报异常退出
        """
        data = self.select(sql_cmd)
        if data is None or (data is not None and data.empty):
            logger.warning(f"Empty dataframe:{sql_cmd}")
            raise ValueError('Empty dataframe')
        return data

    def rselect(self, sql_cmd):  # 解决数据延迟问题
        try:
            data = self.__rselect__(sql_cmd)
        except:
            data = pd.DataFrame()
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

    @retry(Exception, delay=delay, tries=tries)  # jitter=(1, 3))
    def __rselect_sql__(self, sql_cmd):  # 执行select语句
        """
        带重试次数的select_sql
        """
        data = self.select_sql(sql_cmd)
        if data is None or (data is not None and data.empty):
            logger.warning(f"Empty dataframe:{sql_cmd}")
            raise ValueError('Empty dataframe')
        return data

    def rselect_sql(self, sql_cmd):  # 解决数据延迟问题
        try:
            data = self.__rselect_sql__(sql_cmd)
        except:
            data = pd.DataFrame()
        return data

    def execute(self, sql_cmd):  # 执行insert、update、delete、call 语句，简单的select/with语句
        sql_cmd = sql_cmd.strip()
        self.connect()
        cur = self.conn.cursor()
        if sql_cmd[0:5].lower() == 'call ':  # 执行存储过程
            self.conn.autocommit = True
        try:
            cur.execute(sql_cmd)
        except Exception as e:
            logger.error(f"ERROR: execute_sql执行sql出错:{sql_cmd} , {e}")
            self.close()
            return 1
        if sql_cmd[0:6].lower() == 'select' or sql_cmd[0:4].lower() == 'with':
            data = pd.DataFrame(cur.fetchall())  # 无列名
            if not data.empty:
                col_name = []  # 创建一个空列表以存放列名
                for c in cur.description:  # 类似 desc table_name返回结果
                    col_name.append(c[0])  # 循环提取列名，并添加到col空列表
                data.columns = col_name
                # data = data.convert_dtypes()
            self.close()
            return data
        self.commit()
        self.close()
        return 0

    @retry(Exception, delay=delay, tries=tries)  # jitter=(1, 3))
    def __rexecute__(self, sql_cmd):  # 执行insert、update、delete、call 语句
        """
        带重试次数的execute
        """
        data = self.execute(sql_cmd)
        if type(data) != 'int':
            if data.empty:
                logger.warning(f"Empty dataframe:{sql_cmd}")
                raise ValueError('Empty dataframe')
        return data

    def rexecute(self, sql_cmd):  # 解决数据延迟问题
        try:
            data = self.__rexecute__(sql_cmd)
        except:
            data = pd.DataFrame()
        return data

    '''
    批量插入 
    data：为dataframe数据，size：为每次提交的记录大小
    sql示例：  "insert into table(username,password,userid) values(%s,%s,%s)"
    '''
    def batchInsert(self, sql, data, size):
        data = data.where(data.notnull(), None)
        ret = 0
        psycopg_pool = PooledDB(psycopg2, mincached=5, blocking=True, user=self.user,
                                password=self.password, database=self.database, host=self.host,
                                port=self.port)
        try:
            connection = psycopg_pool.connection()
            cursor = connection.cursor()
        except Exception as e:
            logger.error(f"ERROR: batchInsert connection, {e}")
            return 1
        cycles = math.ceil(data.shape[0] / size)
        for i in range(cycles):
            val = data[i * size:(i + 1) * size].values
            try:
                cursor.executemany(sql, val)
                connection.commit()
            except Exception as e:
                logger.error(f"ERROR: batchInsert, {e}\n{val}")
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
        100w条数据入库时间对比：
            # insert ... 0:06:11.323474
            # save   ... 0:01:21.860766
            # copy   ... 0:00:11.978357
        """
        table_columns = str(list(data.columns.array))[1:-1].replace("'", "\"")
        table_values = '%s'
        for i in range(0, len(data.columns) - 1):
            table_values = table_values + ',' + '%s'
        sql = "insert into {}({}) values ({})".format(table_name, table_columns, table_values)
        if self.batchInsert(sql, data, size) == 1:
            logger.error(f"ERROR: insert_to_DB_from_DF ,{table_name}, {sql}")
            return 1
        return 0

    def copy_to_DB_from_DF(self, data, table_name):
        """
        使用psql命令批量从dataframe拷贝到数据库表，以加快速度，适用于数据量较大
        :param data: dataframe
        :param table_name: table name must have schema
        :return:
        100w条数据入库时间对比：
            # insert ... 0:06:11.323474
            # save   ... 0:01:21.860766
            # copy   ... 0:00:11.978357
        """
        if data is None or len(data) == 0:
            logger.warning("数据集为空，不处理")
            return

        file_name = configure_file.configure_file().get_item('environment variables', 'work_dir')+f'/temp_data_{table_name}.csv'
        data.to_csv(file_name, encoding='utf_8', index=False, sep=",")

        # 导入新数据
        cmd = self.psql_cmd + f""" "COPY {table_name} FROM '{file_name}' WITH csv header DELIMITER ',' ENCODING 'utf8' " """
        returncode, out = self.execute_cmd(cmd)
        if returncode != 0:
            logger.error(f"执行导入数据出错，退出系统：{cmd}，{out}")
            raise SystemExit(f'execute {cmd} err :{out}')
        os.remove(file_name)

    def copy_to_DF_from_DB(self, select_sql):
        """
        使用psql命令批量从数据库表拷贝到dataframe，以加快速度，适用于数据量较大
        :param data: dataframe
        :param select_sql: select * from table_name, table name must have schema
        :return:
        """
        file_name = f"""/tmp/temp_data_{int(time.time()*1000*1000)}.csv"""
        # 导出数据
        cmd = self.psql_cmd + f""" "COPY ( {select_sql} ) TO '{file_name}' WITH csv header ;" """
        returncode, out = self.execute_cmd(cmd)
        if returncode != 0:
            logger.error(f"执行导出数据出错，退出系统：{cmd}，{out}")
            raise SystemExit(f'execute {cmd} err :{out}')
        data = pd.read_csv(file_name, index_col=False)
        os.remove(file_name)
        return data

    def save_to_DB_from_DF(self, data: pd.DataFrame, tablename, schema):  # 把dataframe保存为数据库表
        """
        :param data: dataframe
        :param tablename: save to table name
        :param schema: schema of table
        :return: int , <>0 is failed
        使用to_sql进行数据入库
        若表不存在则直接创建并追加数据，字段名称必须一致，顺序可以不一致
        100w条数据入库时间对比：
            # insert ... 0:06:11.323474
            # save   ... 0:01:21.860766
            # copy   ... 0:00:11.978357
        https://blog.csdn.net/xc_zhou/article/details/118829588
        """
        from sqlalchemy import create_engine
        if data is None or data.empty:
            logger.info(f"数据集为None或空，不进行插入操作")
        engine = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        engine = create_engine(engine, echo=False)
        try:
            data.to_sql(tablename, con=engine, schema=schema, if_exists="append", index=False)
        except Exception as e:
            logger.error(f"ERROR: to_sql执行出错:{e}")
            engine.dispose()  # 关闭engine
            return 1

        engine.dispose()  # 关闭engine
        return 0

    def check_DB_deadlock(self, state='ExclusiveLock'):  # 检查数据库排他锁
        """
        select pg_terminate_backend(pid) from pg_stat_activity
        where (now() - backend_start) > '00:10:00' and pid != (select pg_backend_pid()); --强杀连接超过10分钟的事务 不要杀死自己的进程
        """
        sql_command = f"""select a."Id",a."User",a."Host",a."Port",a."db",a."Command",a."State",a."Time",a."Info" from (
    SELECT a.pid AS "Id", a.usename AS "User", a.client_addr AS "Host", a.client_port AS "Port", datname AS "db", 
        query AS "Command", l.mode AS "State", query_start AS "Time", 
        CASE WHEN c.relname IS NOT NULL THEN 'Locked Object: ' || c.relname ELSE 'Locked Transaction: ' || l.virtualtransaction END AS "Info" 
    FROM pg_stat_activity a 
    LEFT JOIN pg_locks l ON a.pid = l.pid 
    LEFT JOIN pg_class c ON l.relation = c.oid 
    ORDER BY a.pid ASC, c.relname ASC
    ) a  where a."State" like '%{state}%'"""
        data = self.select(sql_command)
        data.columns = ["id", "User", "Host", "Port", "db", "Command", "State", "Time", "Info"]
        data = data[pd.notnull(data['Time'])]
        data['Time'] = data.Time.dt.tz_localize(tz=None)
        data['Duration'] = pd.Timestamp(datetime.datetime.now()) - data['Time']
        data['Seconds'] = data.Duration.array.seconds
        data['Command'] = data.Command.apply(lambda x: x.replace('\n', ' ').replace('\t', ' ').replace('\r', ' '))
        data['Command'] = data.Command.apply(lambda x: re.sub(' +', ' ', x))
        return data

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

    def desc_table(self, schema, table_name=''):
        """
        读取schema下面的表结构，table_name为空时取所有表
        :return:
        """
        if table_name == '':
            table_name = '%'
        # 判断是否主键
        sql =f"""
        SELECT
            col.ordinal_position::integer,
            col.table_catalog,
            col.table_schema,
            col.TABLE_NAME,
            col.COLUMN_NAME,
            col.column_default,
            col.data_type,
            col.udt_name,
            col.character_maximum_length::integer,
            col.numeric_precision::integer,
            col.numeric_precision_radix::integer,
            col.numeric_scale::integer,
            col.is_identity,
            col.is_nullable,
            (
            CASE
                WHEN (
                SELECT COUNT
                    ( pg_constraint.* ) 
                FROM
                    pg_constraint
                    INNER JOIN pg_class ON pg_constraint.conrelid = pg_class.oid
                    INNER JOIN pg_attribute ON pg_attribute.attrelid = pg_class.oid 
                    AND pg_attribute.attnum = ANY ( pg_constraint.conkey )
                    INNER JOIN pg_type ON pg_type.oid = pg_attribute.atttypid 
                WHERE
                    pg_class.relname = C.relname 
                    AND pg_constraint.contype = 'p' 
                    AND pg_attribute.attname = A.attname 
                    ) > 0 THEN
                TRUE ELSE FALSE 
                END 
                ) AS pri_key,
                ( SELECT description FROM pg_description WHERE objoid = A.attrelid AND objsubid = A.attnum ) AS description 
        FROM
            pg_class C,
            pg_attribute A,
            (
            SELECT C
                .oID,
                n.nspname AS table_schema 
            FROM
                pg_catalog.pg_class
                C LEFT JOIN pg_catalog.pg_namespace n ON n.oID = C.relnamespace 
            WHERE
                ( C.relname ) = LOWER ( '{table_name}' ) 
                AND ( n.nspname ) = LOWER ( '{schema}' ) 
            ) b,
            pg_type T,
            information_schema.COLUMNS AS col 
        WHERE
            A.attnum > 0 
            AND A.attrelid = C.oid 
            AND A.atttypid = T.oid 
            AND A.attrelID = b.oID 
            AND col.TABLE_NAME = C.relname 
            AND col.table_schema = b.table_schema 
            AND col.COLUMN_NAME = A.attname 
        ORDER BY
            col.table_schema DESC,
            col.TABLE_NAME DESC,
            A.attnum ASC
        """
        sql = f"""
        SELECT
            col.table_catalog,
            col.table_schema,
            col.TABLE_NAME,
            des2.description as table_desc,
            col.ordinal_position,
            col.COLUMN_NAME,
            col.data_type,
            col.character_maximum_length,
            col.numeric_precision,
            col.numeric_precision_radix,
            col.numeric_scale,
            col.is_nullable,
            col.column_default,
            des.description
        FROM
            information_schema.COLUMNS col
            LEFT JOIN pg_description des ON ( '{schema}.' || col.TABLE_NAME ) :: regclass = des.objoid 
            AND col.ordinal_position = des.objsubid
            LEFT JOIN pg_description des2 ON ( '{schema}.' || col.TABLE_NAME ) :: regclass = des2.objoid 
            AND des2.objsubid = 0 
        WHERE
            table_schema = '{schema}' 
            AND TABLE_NAME like '{table_name}' 
        ORDER BY
            col.table_schema,
            col.TABLE_NAME,
            col.ordinal_position
        """
        df = self.select(sql)
        if not df.empty:
            float_col = ['character_maximum_length', 'numeric_precision', 'numeric_precision_radix', 'numeric_scale']
            # df[float_col] = df[float_col].astype('Int64')
            df[float_col] = df[float_col].fillna(0).astype(int)
            # print(df)
        return df


def performance_testing():
    """
    数据导入性能测试：测试表
        CREATE TABLE test_linlefeng (
        k1 BIGINT,
        k2 FLOAT,
        k3 char(20),
        k4 BIGINT
        )
    :return:  函数    用时
        insert ... 0:06:11.323474
        save   ... 0:01:21.860766
        copy...    0:00:11.978357
    """
    db = database('bigdata_db')
    k1=[]
    k2 = []
    k3 = []
    k4 = []
    for i in range(10):
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
    import datetime
    print('---------------------------')
    s = datetime.datetime.now()
    db.insert_to_DB_from_DF(data, 'test.test_linlefeng', 1000)
    print('insert ...', datetime.datetime.now()-s)
    s = datetime.datetime.now()
    db.save_to_DB_from_DF(data, 'test_linlefeng', 'test')
    print('save   ...', datetime.datetime.now()-s)    # print(data)
    s = datetime.datetime.now()
    db.copy_to_DB_from_DF(data, 'test.test_linlefeng')
    print('copy   ...', datetime.datetime.now()-s)


if __name__ == '__main__':
    # pass

    # db1 = database('bigdata_db')
    # sql = """select id,name,to_char(ctime,'yyyy-mm-dd hh24:mi:ss') ctime from test.temp_test"""
    # sql = """select id,name,ctime::timestamp from test.temp_test"""
    # data = db1.select(sql)
    # data.ctime = data.ctime.astype('datetime64[s]')
    # print(data)
    #
    # with database('order_slave_db', '2') as db2:
    #     sql = """SELECT count(*) FROM order_info"""
    #     data = db2.select(sql)
    # print(data)

    db = database('bigdata_db')
    # df = db1.select('select * from test.asdf')
    # df = df[['sid','sids']]

    # col_name = data.columns.tolist()
    # col_name.insert(col_name.index('mid')+1, 'abcd')  # 在 B 列前面插入
    # data = data.reindex(columns=col_name)
    # print(data)
    sql = """
select c.mname,a.gid ,case when b.grade_code = 1 then '银卡' when b.grade_code = 2 then '金卡' when b.grade_code = 3 then '白金卡' else end as grade_code,b.point ,count(*) cnt,sum(a.origin_amount) origin_amount
from u_order.order_info_view a
left join public.dwd_user_info b on a.gid = b.gid
left join public.ods_mall c on a.mid = c.id
where a.status = 2 and a.sid not in (
0,18310,18312,19857,19858,19859,19860,19861,19862,19863,19864,19865,19866,19950,19952,19953,20547,20625,23514,24277,1505,1654,1663,12294,14478,14534,14947,16634,19876,19877,19878,20615,1694,14171,15239,15240,15241,15242,15312,19879,19880,19881,20616,1693,5079,12109,14030,15313,19882,19883,19884,20617,15314,19886,19887,19888,20102,20618,8599,11540,11541,11542,11550,11552,11553,11554,11555,11556,12145,15317,19890,19891,19892,20619,7889,15318,19893,19894,19897,20620,8177,8251,12134,15213,15319,19899,19900,19901,20621,15251,15252,15253,15315,19902,19903,19904,20622,9863,10284,15320,18684,19905,19906,19907,20623,24278,13259,13928,15321,19908,19910,19911,20624,23515
) and a.ctime::date >= '2022-01-01' and a.mid != 18
group by c.mname,a.gid,b.grade_code,b.point    
    """
    data = db.copy_to_DF_from_DB(sql)
    db.save_to_DB_from_DF(data, "temp111", "test")

