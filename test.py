#!/usr/local/bin/python3

"""
"""

import datetime
import json
import os
import random
import time
from multiprocessing import Pool

import numpy as np
import pandas as pd
import requests
from retry import retry

import public
import public_umall
import configure_file
import db
import doris


def interesting_zip():  # 有趣的zip函数
    x = [1, 2, 3, 4, 5, 6, 7]
    x0 = [x] * 2
    x1 = list(zip(x0))
    x2 = list(zip(*x0))
    x3 = list(zip(*(iter(x),) * 3))
    x4 = list(zip(*(iter(x),)))
    x5 = list(iter(x))
    x6 = list((iter(x),))
    x7 = list((iter(x),) * 2)
    return


def class_test():
    class Model(object):
        def __init__(self, name):
            self.name1 = name
            self.name2 = 'name2...'
            print(self.name1, self.name2)

        def save(self, force_update=False, force_insert=False):
            if force_update and force_insert:
                print("True:Cannot perform both operations")
            if force_update == False and force_insert == False:
                print("False:Cannot perform both operations")
            if force_update:
                print("Updated an existing record")
            if force_insert:
                print("Created a new record")

        def dosomething(self):
            print('dosomething')

    class ChildModel(Model):
        def __init__(self, name):
            self.name1 = name
            self.name3 = 'name3...'
            super(ChildModel, self).__init__(name)
            print(self.name1, self.name3)

        def save(self, *args, **kwargs):
            if self.name1 == 'abcd':
                super(ChildModel, self).save(*args, **kwargs)
            else:
                return None

    c = ChildModel('abcd')
    c.save(force_insert=True)
    c.save(force_update=True)
    c.save(force_update=True, force_insert=True)
    c.save()
    c.dosomething()
    print(c.name1, c.name2, c)


def time_test():
    import time
    a = time.time()
    e = datetime.datetime.now()
    print('time.time():', type(a), a)
    a = time.localtime()
    print('time.localtime():', type(a), a)
    b = time.strftime('%Y-%m-%d %X', a)
    print('time.strftime:', type(b), b)
    c = time.strptime(b, '%Y-%m-%d %X')
    print('time.strptime:', type(c), c)
    d = time.mktime(c)
    print('time.mktime(c):', type(d), d)

    print('datetime.datetime.now():', type(e), e)
    pass


def plot_roc_curve_test():
    import matplotlib.pyplot as plt
    from sklearn import datasets, metrics, model_selection, svm
    X, y = datasets.make_classification(random_state=0)
    X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y, random_state=0)
    clf = svm.SVC(random_state=0)
    clf.fit(X_train, y_train)
    metrics.plot_roc_curve(clf, X_test, y_test)
    plt.show()


import psutil


def proc_exist(process_name):
    pl = psutil.pids()
    for pid in pl:
        # print(pid, psutil.Process(pid).name(), '\t', psutil.Process(pid).cmdline())
        if psutil.Process(pid).name() == process_name:
            print('-' * 30, pid, psutil.Process(pid).name(), '\t', psutil.Process(pid).cmdline())
            # return pid


def with_usage():
    class Test:
        def __init__(self):
            print('__init__() is call!')

        def __enter__(self):
            print('__enter__() is call!')
            return self

        def dosomething(self):
            print('dosomethong!')
            x = 1 / 0

        def __exit__(self, exc_type, exc_value, traceback):
            print('__exit__() is call!')
            print(f'type:{exc_type}')
            print(f'value:{exc_value}')
            print(f'trace:{traceback}')
            print('__exit()__ is over!')
            return True

    with Test() as sample:
        if isinstance(sample, Test):
            sample.dosomething()
    print('The end ...')


def Nan_None():
    dict = {'name': ['小明123', None, '狗蛋', '铁柱'], '身高': [17, 20, 5, np.nan],
            '年龄': [17, 20, 5, None], 'sex': ['男', '女', np.nan, '男']}
    data = pd.DataFrame(dict)
    print(data.isna())
    print(data.dtypes, '\n', data)
    data = data.convert_dtypes()
    print(data.dtypes, '\n', data)
    print(data.isnull().sum())
    data.fillna(value={'年龄': 0, 'sex': ''}, inplace=True)
    print(data.dtypes, '\n', data)
    print("type(None)=%s, type(np.nan)=%s, type(np.NaN)=%s, type(np.NAN)=%s" % (
        type(None), type(np.nan), type(np.NaN), type(np.NAN)))
    print("None=%s, np.nan=%s, np.NaN=%s, np.NAN=%s" % (None, np.nan, np.NaN, np.NAN))

    a, b, c, d = np.nan, np.NaN, np.NAN, None
    print(f"1\t{a} is {b} : {a is b}")
    print(f"2\t{a} is {c} : {a is c}")
    print(f"3\t{b} is {c} : {b is c}")
    print(f"4\t{a} == {b} : {a == b}")
    print(f"5\t{a} == {d} : {a == d}")
    print(f"6\t{a} is {d} : {a is d}")
    print(f"7\t{d} is None : {d is None}")
    print(f"7\t{d} == None : {d == None}")
    print(f"8\t{d} is {a} : {d is a}")
    # print(a, b, c, a is b, a is c, b is c, a == b, a == d, a is d, d is None, d is a)


def sqoop2():
    #     Name | Version | Class | Supported
    #     Directions |
    # +------------------------+---------+------------------------------------------------------------+----------------------+
    # | generic - jdbc - connector | 1.99
    # .7 | org.apache.sqoop.connector.jdbc.GenericJdbcConnector | FROM / TO |
    # | kite - connector | 1.99
    # .7 | org.apache.sqoop.connector.kite.KiteConnector | FROM / TO |
    # | oracle - jdbc - connector | 1.99
    # .7 | org.apache.sqoop.connector.jdbc.oracle.OracleJdbcConnector | FROM / TO |
    # | ftp - connector | 1.99
    # .7 | org.apache.sqoop.connector.ftp.FtpConnector | TO |
    # | hdfs - connector | 1.99
    # .7 | org.apache.sqoop.connector.hdfs.HdfsConnector | FROM / TO |
    # | kafka - connector | 1.99
    # .7 | org.apache.sqoop.connector.kafka.KafkaConnector | TO |
    # | sftp - connector | 1.99
    # .7 | org.apache.sqoop.connector.sftp.SftpConnector | TO |
    # +------------------------+---------+------------------------------------------------------------+----------------------+

    url = "http://172.16.17.101:12000/sqoop/version"
    res = requests.get(url=url)
    print(res, res.text)
    url = "http://172.16.17.101:12000/sqoop/v1/connector/oracle-jdbc-connector"
    res = requests.get(url=url)
    print(res, res.text)
    url = "http://172.16.17.101:12000/sqoop/v1/connector/hdfs-connector"
    res = requests.get(url=url)
    print(res, res.text)
    url = "http://172.16.17.101:12000/sqoop/v1/driver"
    res = requests.get(url=url)
    print(res, res.text)

    newlink = {
        "link": {
            "id": -1,
            "enabled": True,
            "link-config-values": [{
                "id": 1,
                "inputs": [{
                    "id": 1,
                    "name": "linkConfig.uri",
                    "value": "hdfs%3A%2F%2Fvbsqoop-1.ent.cloudera.com%3A8020%2Fuser%2Froot%2Fjob1",
                    "type": "STRING",
                    "size": 255,
                    "sensitive": False
                }],
                "name": "testInput",
                "type": "LINK"
            }],
            "update-user": "root",
            "name": "testLink",
            "creation-date": 1415202223048,
            "connector-name": "hdfs-connector",
            "update-date": 1415202223048,
            "creation-user": "root",
            "connector-id": 7
        }
    }
    print(json.dumps(newlink))
    res = requests.post(url="http://172.16.17.101:12000/sqoop/v1/link", data=json.dumps(newlink))
    print(res, res.text)


class GlobalMap:
    # 拼装成字典构造全局变量  借鉴map  包含变量的增删改查
    map = {}

    def set_map(self, key, value):
        if (isinstance(value, dict)):
            value = json.dumps(value)
        self.map[key] = value

    def set(self, **keys):
        try:
            for key_, value_ in keys.items():
                self.map[key_] = str(value_)
                logger.debug(key_ + ":" + str(value_))
        except BaseException as msg:
            logger.error(msg)
            raise msg

    def del_map(self, key):
        try:
            del self.map[key]
            return self.map
        except KeyError:
            logger.error("key:'" + str(key) + "'  不存在")

    def get(self, *args):
        try:
            dic = {}
            for key in args:
                if len(args) == 1:
                    dic = self.map[key]
                    logger.debug(key + ":" + str(dic))
                elif len(args) == 1 and args[0] == 'all':
                    dic = self.map
                else:
                    dic[key] = self.map[key]
            return dic
        except KeyError:
            logger.warning("key:'" + str(key) + "'  不存在")
            return 'Null_'


def draw_venn_graph():
    import matplotlib.pyplot as plt
    from matplotlib_venn import venn2, venn2_circles  # pip install matplotlib_venn 或者conda install matplotlib_venn)
    work_dir = "/home/test"
    my_dpi = 150
    plt.rc('font', family='SimSun', size=6)
    plt.figure(figsize=(300 / my_dpi, 200 / my_dpi), dpi=my_dpi)  # 控制图尺寸的同时，使图高分辨率（高清）显示
    g = venn2(subsets=[100.5, 50, 5],  # 绘图数据集
              set_labels=('优mall会员', '企微人数'),  # 设置组名
              set_colors=("#098154", "#c72e29"),  # 设置圈的颜色，中间颜色不能修改
              alpha=0.6,  # 透明度
              normalize_to=1.0  # venn图占据figure的比例，1.0为占满
              )

    plt.annotate('优mall独有',
                 color='#098154',
                 xy=g.get_label_by_id('10').get_position() - np.array([0, 0.05]),
                 xytext=(-30, 40),
                 ha='center', textcoords='offset points',
                 bbox=dict(boxstyle='round,pad=0.5', fc='#098154', alpha=0.6),  # 注释文字底纹
                 arrowprops=dict(arrowstyle='-|>', connectionstyle='arc3,rad=0.5', color='#098154')  # 箭头属性设置
                 )

    plt.annotate('企微独有',
                 color='#c72e29',
                 xy=g.get_label_by_id('01').get_position() + np.array([0, 0.05]),
                 xytext=(20, 35),
                 ha='center', textcoords='offset points',
                 bbox=dict(boxstyle='round,pad=0.5', fc='#c72e29', alpha=0.6),
                 arrowprops=dict(arrowstyle='-|>', connectionstyle='arc3,rad=0.5', color='#c72e29')
                 )

    plt.annotate('共有',
                 color='black',
                 xy=g.get_label_by_id('11').get_position() + np.array([0, 0.05]),
                 xytext=(0, 35),
                 ha='center', textcoords='offset points',
                 bbox=dict(boxstyle='round,pad=0.5', fc='grey', alpha=0.6),
                 arrowprops=dict(arrowstyle='-|>', connectionstyle='arc3,rad=-0.5', color='black')
                 )
    plt.savefig(work_dir + "/venn", dpi=200, bbox_inches='tight', transparent=True)
    plt.show()
    plt.close()
    # cdn_url = upload_file.upload_to_CDN(work_dir + "/venn.png")
    # if cdn_url == '':
    #     logger.error("ERROR: 图片上传CDN失败,及时查找原因。")
    os.remove(work_dir + "/venn.png")


# from gooey import Gooey, GooeyParser
# # from message import display_message
# @Gooey(dump_build_config=True, program_name="Widget Demo")
def main_app():
    desc = "Example application to show Gooey's various widgets"
    file_help_msg = "Name of the file you want to process"

    my_cool_parser = GooeyParser(description=desc)

    my_cool_parser.add_argument(
        "FileChooser", help=file_help_msg, widget="FileChooser")
    my_cool_parser.add_argument(
        "DirectoryChooser", help=file_help_msg, widget="DirChooser")
    my_cool_parser.add_argument(
        "FileSaver", help=file_help_msg, widget="FileSaver")
    my_cool_parser.add_argument(
        "MultiFileSaver", help=file_help_msg, widget="MultiFileChooser")
    my_cool_parser.add_argument("directory", help="Directory to store output")

    my_cool_parser.add_argument('-d', '--duration', default=2,
                                type=int, help='Duration (in seconds) of the program output')
    my_cool_parser.add_argument('-s', '--cron-schedule', type=int,
                                help='datetime when the cron should begin', widget='DateChooser')
    my_cool_parser.add_argument(
        "-c", "--showtime", action="store_true", help="display the countdown timer")
    my_cool_parser.add_argument(
        "-p", "--pause", action="store_true", help="Pause execution")
    my_cool_parser.add_argument('-v', '--verbose', action='count')
    my_cool_parser.add_argument(
        "-o", "--overwrite", action="store_true", help="Overwrite output file (if present)")
    my_cool_parser.add_argument(
        '-r', '--recursive', choices=['yes', 'no'], help='Recurse into subfolders')
    my_cool_parser.add_argument(
        "-w", "--writelog", default="writelogs", help="Dump output to local file")
    my_cool_parser.add_argument(
        "-e", "--error", action="store_true", help="Stop process on error (default: No)")
    verbosity = my_cool_parser.add_mutually_exclusive_group()
    verbosity.add_argument('-t', '--verbozze', dest='verbose',
                           action="store_true", help="Show more details")
    verbosity.add_argument('-q', '--quiet', dest='quiet',
                           action="store_true", help="Only output on error")

    args = my_cool_parser.parse_args()
    # display_message()


dict1 = {
    'name': ['小明123', '小红12', None, '铁柱', '小明123', None, '狗蛋', '铁柱', '小明123', '小红12', '狗蛋', '铁柱', '小明123', '小红12',
             '狗蛋', '铁柱'],
    'sex': ['男', '女', None, '男', '男', '女', '', '男', '男', '女', '', '男', '男', '女', '', '男'],
    'age': [100, 200, 300, np.NaN, 100, 200, 300, 200, 100, 200, 300, np.NaN, 100, 200, 300, None]}

data = pd.DataFrame(dict1)


if __name__ == '__main__':
    pass
    logger = public.bigdata_logger(f'test.log')
    logger.info(f'test starting ')
    #
    # df2 = pd.DataFrame([('parrot', 24.0, 'second'),
    #                     ('lion', 80.5, 1),
    #                     ('monkey', np.nan, None)],
    #                    columns=('name', 'max_speed', 'rank'))
    # df2 = df2.convert_dtypes()
    # print(df2)
    # simi_df = pd.DataFrame(columns=['id1', 'sname1', 'id2', 'sname2', 'simi'])
    # simi_df.loc[simi_df.shape[0]] = [1, ('c','d'), 1, {'a','b'}, [1,1]]
    # s1 = simi_df.sname1[0]
    # s2 = simi_df.sname2[0]
    # s3 = simi_df.simi[0]
    # print(simi_df)
    # print(type(s1), s1)
    # print(type(s2), s2)
    # print(type(s3), s3)
    # exit()

