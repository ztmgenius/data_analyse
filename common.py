#!/usr/local/bin/python3
"""
该文件为兼容老程序而保留，新程序应用public.py
所有关于数据库操作的函数都是为了兼容先前程序，新程序都调用db.py类进行操作
所有关于读取配置文件的函数都是为了兼容先前程序，新程序都调用configure_file.py类进行操作
"""
import subprocess
import os
import pandas as pd
import psycopg2
from dbutils.pooled_db import PooledDB
import math
import time
import datetime
from datetime import timedelta
from datetime import datetime
import re
import multiprocessing
import logging.handlers
import configparser


def get_sys_config():
    local_path = os.path.dirname(os.path.realpath(__file__))  # 实例化conf对象
    conf = configparser.ConfigParser()  # conf读取配置文件:务必指定编码方式，否则windows下默认以gbk读取utf-8格式的配置文件将会报错
    conf.read(local_path + "/system.conf", encoding="utf-8")

    db = 'bigdata_db'
    postgres_host = conf.get(db, "host")
    postgres_port = conf.get(db, "port")
    postgres_user = conf.get(db, "user")
    postgres_password = conf.get(db, "password")
    postgres_datebase = conf.get(db, "database")
    connect_db = conf.get(db, "connect_db")

    env = 'environment variables'
    work_dir = conf.get(env, "work_dir")
    log_dir = conf.get(env, "log_dir")
    mid = conf.get(env, "mid")
    mid = [m.strip() for m in mid.split(',')]

    file_dir = work_dir+'png/'
    # 创建目录
    if not os.path.exists(file_dir):
        os.mkdir(file_dir)
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    return work_dir, log_dir, mid, connect_db, postgres_host, postgres_port, postgres_user, postgres_password, postgres_datebase

work_dir, log_dir, mid, connect_db, postgres_host, postgres_port, postgres_user, postgres_password, postgres_datebase = get_sys_config()
# postgres_host 数据库地址
# postgres_port 数据库端口
# postgres_user 数据库用户名
# postgres_password 数据库密码
# postgres_datebase 数据库名字
# work_dir = os.getcwd() + '/'
# log_dir = work_dir + 'log/'
# connect_db = 'psql -h 172.16.17.200 -p 5432 -U postgres -d postgres -c '


def get_sys_var(section, var_name):  # 读取单个系统变量
    local_path = os.path.dirname(os.path.realpath(__file__))  # 实例化conf对象
    conf = configparser.ConfigParser()  # conf读取配置文件:务必指定编码方式，否则windows下默认以gbk读取utf-8格式的配置文件将会报错
    conf.read(local_path + "/system.conf", encoding="utf-8")

    value = conf.get(section, var_name)

    return value


def get_CPU_num():  # 服务器cpu个数
    # CPUs = int(execute_cmd("""cat /proc/cpuinfo| grep "physical id"| sort| uniq| wc -l""")[1])
    CPUs = multiprocessing.cpu_count()
    return CPUs


def execute_cmd(cmd):
    p = subprocess.Popen(cmd,
                         shell=True,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    if p.returncode != 0:
        return p.returncode, stderr
    return p.returncode, stdout


def get_config():
    file_dir = work_dir+'png/'
    # 创建目录
    if not os.path.exists(file_dir):
        os.mkdir(file_dir)
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    return work_dir, connect_db, postgres_host, postgres_port, postgres_user, postgres_password, postgres_datebase


def get_mallID():
    return mid


def get_shop_info(mid, online=False):  # 取店铺名称方法：shop_data.loc[sid][0]
    if online:
        sql = "select id,sname from ods_shop where mid= {} and is_online order by id".format(int(mid))
    else:
        sql = "select id,sname from ods_shop where mid= {} order by id".format(int(mid))
    shop = select_sql(sql)
    shop = shop.set_index(['id'])
    return shop


def delete_history_from_db(from_date='2021-01-01', to_date='2021-03-28'):
    conn = psycopg2.connect(host=postgres_host, user=postgres_user, password=postgres_password,
                            port=postgres_port, database=postgres_datebase)
    cur = conn.cursor()
    table_name = ["public.dwa_shop_sale_graph", "public.dwa_association_rule", "public.dwa_association_rule_graph",
                  "public.dwa_consumption_level", "public.dwa_fraud_group", "public.dwa_shop_sale_graph",
                  "public.dwa_user_retention_graph"]
    for name in table_name:
        delete_sql = "delete from " + name + " where to_char(ctime,\'yyyy-mm-dd\')>=\'" + from_date \
                 + "\' and to_char(ctime,\'yyyy-mm-dd\')<=\'" + to_date + "\'"
        cur.execute(delete_sql)
        conn.commit()
    conn.close()


def add_month(ym, n, sep='-'):  # 对YYYYMM的字符型年月，进行加减法运算，n:为负数时减法，sep：分隔符
    if len(ym) < 6:
        return ''
    if sep == '':
        year = ym[:4]
        month = ym[4:6]
    else:
        year, month = ym.split(sep)
    if int(month) > 12:
        return ''
    m = abs((int(month) + n) % 12)
    y = int(year) + (int(month) + n) // 12
    if m == 0:
        m = 12
        y -= 1

    return str(y) + sep + str(m).zfill(2)


def error_call_back(value):
    import traceback
    print(f"{time.strftime('%Y-%m-%d %X', time.localtime())} Pool callback ERROR:{value}, Pid:{os.getpid()}, PPid:{os.getppid()}")
    err = traceback.format_exception(type(value), value, value.__traceback__)
    print(err)


def check_DB_deadlock():  # 检查数据库排他锁
    """
    select pg_terminate_backend(pid) from pg_stat_activity
    where (now() - backend_start) > '00:10:00' and pid != (select pg_backend_pid()); --强杀连接超过10分钟的事务 不要杀死自己的进程
    """
    sql_command = """select a."Id",a."User",a."Host",a."Port",a."db",a."Command",a."State",a."Time",a."Info" from (
SELECT a.pid AS "Id", a.usename AS "User", a.client_addr AS "Host", a.client_port AS "Port", datname AS "db", 
    query AS "Command", l.mode AS "State", query_start AS "Time", 
    CASE WHEN c.relname IS NOT NULL THEN 'Locked Object: ' || c.relname ELSE 'Locked Transaction: ' || l.virtualtransaction END AS "Info" 
FROM pg_stat_activity a 
LEFT JOIN pg_locks l ON a.pid = l.pid 
LEFT JOIN pg_class c ON l.relation = c.oid 
ORDER BY a.pid ASC, c.relname ASC
) a where a."State" like '%ExclusiveLock%' """  # where a."State" like '%ExclusiveLock%'
    data = execute_sql(sql_command)
    data.columns = ["id", "User", "Host", "Port", "db", "Command", "State", "Time", "Info"]
    data = data[pd.notnull(data['Time'])]
    data['Time'] = data.Time.dt.tz_localize(tz=None)
    data['Duration'] = pd.Timestamp(datetime.now()) - data['Time']
    data['Seconds'] = data.Duration.array.seconds
    data['Command'] = data.Command.apply(lambda x: x.replace('\n', ' ').replace('\t', ' ').replace('\r', ' '))
    data['Command'] = data.Command.apply(lambda x: re.sub(' +', ' ', x))
    return data


def select_sql(sql_command):
    """
    与execute_sql的区别：
        1、data带有列名，无需定义
        2、时间戳字段不带时区需自行转换！！！ 方法：data['ctime'] = data.ctime.dt.tz_convert('Asia/Shanghai')
        3、适用于数据量不是很大
    :param sql_command: 数据库select语句
    :return: dataFrame
    """
    sql_command = sql_command.strip()
    data = pd.DataFrame()
    conn = psycopg2.connect(host=postgres_host, user=postgres_user, password=postgres_password,
                            port=postgres_port, database=postgres_datebase)
    try:
        data = pd.read_sql(sql_command, conn)  # 有列名
    except Exception as e:
        print("ERROR: select_sql执行出错:{0} , {1}".format(sql_command, e))
    conn.close()
    return data

def execute_sql(sql_command):
    sql_command = sql_command.strip()
    conn = psycopg2.connect(host=postgres_host, user=postgres_user, password=postgres_password,
                            port=postgres_port, database=postgres_datebase)
    cur = conn.cursor()
    try:
        cur.execute(sql_command)
    except Exception as e:
        print("ERROR: execute_sql执行sql出错:{0} , {1}".format(sql_command, e))
        conn.close()
        return 1
    if sql_command[0:6].lower() == 'select':
        data = pd.DataFrame(cur.fetchall())  # 无列名
        conn.close()
        return data
    conn.commit()
    conn.close()


def call_sql(sql_command):
    sql_command = sql_command.strip()
    conn = psycopg2.connect(host=postgres_host, user=postgres_user, password=postgres_password,
                            port=postgres_port, database=postgres_datebase)
    cur = conn.cursor()
    conn.autocommit = True
    try:
        cur.execute(sql_command)
    except Exception as e:
        print("ERROR:call_sql执行sql出错:{0} , {1}".format(sql_command, e))
    conn.close()
    return


'''
批量插入 
data：为dataframe数据，size：为批量大小
sql示例：  "insert into table(username,password,userid) values(%s,%s,%s)"
'''
def batchInsert(sql, data, size):
    ret = 0
    psycopg_pool = PooledDB(psycopg2, mincached=5, blocking=True, user=postgres_user,
        password=postgres_password, database=postgres_datebase, host=postgres_host,
        port=postgres_port)
    try:
        connection = psycopg_pool.connection()
        cursor = connection.cursor()
    except Exception as e:
        print("ERROR: batchInsert connection, ", e)
        return 1
    cycles = math.ceil(data.shape[0] / size)
    for i in range(cycles):
        val = data[i * size:(i + 1) * size].values
        try:
            cursor.executemany(sql, val)
            connection.commit()
        except Exception as e:
            print("ERROR: batchInsert, ", e, '\n', val)
            connection.rollback()
            ret = 1
    connection.close()
    return ret


def get_dataframe_columnname(data):
    columns_name = []
    for x in data.columns:
        x = x.split('.')[-1]
        columns_name = columns_name + [x]
    return columns_name


def insert_to_DB_from_DF(data, table_name, size):  # data is a DataFrame
    ##获取字段名 可以简化为str(list(data.columns.array))[1:-1].replace("'", "\"")
    table_columns = str(list(pd.DataFrame(data.columns).astype(str)[0]))[1:-1].replace("'", "\"")
    table_values = '%s'
    for i in range(0, len(data.columns) - 1):
        table_values = table_values + ',' + '%s'
    sql = "insert into {}({}) values ({})".format(table_name, table_columns, table_values)
    if batchInsert(sql, data, size) == 1:
        print("ERROR: insert_to_DB_from_DF , ", table_name, sql)
        return 1
    return 0


def copy_to_DB_from_DF(data, table_name):
    if data is None or len(data) == 0:
        print("数据集为空，不处理")
        return

    file_name = work_dir + f'temp_data_{table_name}.csv'
    data.to_csv(file_name, encoding='utf_8_sig', index=False, sep=",")

    # 导入新数据
    cmd = connect_db + f""" "COPY {table_name} FROM '{file_name}' WITH csv header DELIMITER ',' ENCODING 'utf8' " """
    returncode, out = execute_cmd(cmd)
    if returncode != 0:
        raise SystemExit('execute {0} err :{1}'.format(cmd, out))
    os.remove(file_name)


def split_list(init_list, children_list_len):  # 把list等分法拆分为多个list,子list元素个数
    list_of_groups = zip(* (iter(init_list),) * children_list_len)
    end_list = [list(i) for i in list_of_groups]
    count = len(init_list) % children_list_len
    end_list.append(init_list[-count:]) if count !=0 else end_list
    return end_list


def zip_dir(dirname, zipfilename=''):  # dirname:压缩单个文件名或一个目录,zipfilename:压缩后的文件名(带有路径)
    import os, os.path
    import zipfile

    filelist = []
    if os.path.isfile(dirname):
        filelist.append(dirname)
        if zipfilename == '':
            zipfilename = filelist[0]+'.zip'
    else:
        if zipfilename == '':
            zipfilename = dirname + (dirname.strip('/').split('/')[-1]) + '.zip'
        for root, dirs, files in os.walk(dirname):
            for name in files:
                filelist.append(os.path.join(root, name))

    zf = zipfile.ZipFile(zipfilename, "w", zipfile.ZIP_DEFLATED)
    for tar in filelist:
        if os.path.isfile(dirname):
            arcname = dirname.split('/')[-1]
        else:
            arcname = tar[len(dirname):]
        zf.write(arcname)
    zf.close()
    return zipfilename


def dataframe_to_excel(data, filename, sheet_name='sheet1', index_flag=False):  # 把dataframe保存为xlsx文件
    """
        把dataframe保存为xlsx文件
        data    数据表 dataframe类型
        filename    保存的文件名
        sheet_name  sheet名称
        index_flag  是否显示行号
    """
    from pandas.api.types import is_datetime64tz_dtype
    for c in data.columns:
        if is_datetime64tz_dtype(data[c]):
            data[c] = data[c].dt.tz_convert('Asia/Shanghai')
            data[c] = data[c].dt.tz_localize(tz=None)

    writer = pd.ExcelWriter(filename, engine='xlsxwriter')
    data.to_excel(writer, startrow=0, sheet_name=sheet_name, index=index_flag)
    worksheet = writer.sheets[sheet_name]

    # Iterate through each column and set the width == the max length in that column. A padding length of 1 is also added.
    for i, col in enumerate(data.columns):
        column_len = max(
            max([len(str(x).encode('gbk')) for x in data[col]]),  # len of largest item
            len(str(data[col].name).encode('gbk'))  # len of column name/header
        ) + 0  # adding a little extra space
        worksheet.set_column(i, i, column_len)
    writer.save()


def dataframe_to_image(data, filename: str):  # 保存dataframe为图像文件
    """
    :param data: dataframe
    :param filename: save to filename
    :return:
    图片效果不好，再优化
    """
    import matplotlib.pyplot as plt
    from pandas.plotting import table

    ax = plt.subplot(111, frame_on=False)  # no visible frame
    ax.xaxis.set_visible(False)  # hide the x axis
    ax.yaxis.set_visible(False)  # hide the y axis

    table(ax, data, loc='upper right')  # where df is your data frame

    # plt.show()
    plt.savefig(filename)


def dataframe_to_sql(data: pd.DataFrame, tablename, schema):  # 把dataframe保存为数据库表
    """
    :param data: dataframe
    :param tablename: save to table name
    :param schema: schema of table
    :return: int , <>0 is failed
    使用to_sql进行数据入库
    若表不存在则直接创建并追加数据
    """
    from sqlalchemy import create_engine
    engine = f'postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_datebase}'
    engine = create_engine(engine, echo=False)
    try:
        data.to_sql(tablename, con=engine, schema=schema, if_exists="append", index=False)
    except Exception as e:
        print(f"ERROR: to_sql执行出错:{e}")
        return 1
    # df = engine.execute(f"SELECT * FROM {schema}.{tablename}").fetchall()
    # df = pd.DataFrame(df)
    # print(df)
    return 0


# # 日期时间字符串
# st = "2020-11-23 16:10:10"
# # 当前日期时间
# dt = datetime.now()
# # 当前时间戳
# sp = time.time()

# 1.把datetime转成字符串
def datetime_toString(dt, fmt="%Y-%m-%d %H:%M:%S"):
    return dt.strftime(fmt)


# 2.把字符串转成datetime
def string_toDatetime(st, fmt="%Y-%m-%d %H:%M:%S"):
    return datetime.strptime(st, fmt)


# 3.把字符串转成时间戳形式
def string_toTimestamp(st, fmt="%Y-%m-%d %H:%M:%S"):
    return time.mktime(time.strptime(st, fmt))


# 4.把时间戳转成字符串形式，sp为秒
def timestamp_toString(sp, fmt="%Y-%m-%d %H:%M:%S"):
    return time.strftime(fmt, time.localtime(sp))


# 5.把datetime类型转外时间戳形式
def datetime_toTimestamp(dt):
    return time.mktime(dt.timetuple())


def date_add(date_str, days_count=1):
    date_list = time.strptime(date_str, "%Y-%m-%d")
    y, m, d = date_list[:3]
    delta = timedelta(days=days_count)
    date_result = datetime(y, m, d) + delta
    date_result = date_result.strftime("%Y-%m-%d")
    return date_result


def timer_fmt(timer, sep):  #将时分秒格式统一为两位格式 8:1:56 -> 08:01:56  8:23 -> 08:23
    return sep.join([str(int(i)).zfill(2) for i in timer.split(sep)])


def bigdata_logger(log_filename='bigdata.log'):
    import datetime
    logger = logging.getLogger('bigdata_logger')
    logger.setLevel(logging.DEBUG)
    rf_handler = logging.handlers.TimedRotatingFileHandler(log_dir+log_filename, when='midnight', interval=1, backupCount=7, atTime=datetime.time(0, 0, 0, 0))
    rf_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s"))
    f_handler = logging.StreamHandler()  # 标准输出
    # f_handler = logging.FileHandler(work_dir + '/log/error.log', encoding='utf-8')   # 写入日志文件
    f_handler.setLevel(logging.INFO)
    f_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s:%(funcName)s:%(lineno)d - %(message)s"))
    logger.addHandler(rf_handler)
    logger.addHandler(f_handler)
    return logger


if __name__ == '__main__':
    init_list = [1,2,3,4,5,6,7,8,9,1,2,3,45,6,7,8,9,9,1,2,3,4,5,6,7,89,6]
    print(add_month('202201',-2,'') ) # 把list等分法拆分为多个list,子list元素个数

    pass

