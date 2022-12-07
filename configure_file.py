#!/usr/local/bin/python3
# coding=utf-8
"""
定义系统用到的所有配置文件，并提供通用类读取配置文件
Created on 2022-01-04
@author: Lin lefeng
"""
import os
import platform
import configparser
from pathlib import Path

#           编号   名称                       描述
file_list = [(0, 'system.conf',             '系统配置文件'),
             (1, 'config/system.conf',      '其他配置文件')
            ]  # 配置文件列表，不同的业务配置不同的配置文件, 调用configure_file类时，传递编号参数即可


class configure_file(object):  # 配置文件类,传递编号参数调用不同配置文件:configure_file(1)
    def __init__(self, file_type=0):
        self.filename = ''
        for i in file_list:
            if i[0] == file_type:
                self.filename = i[1]
                break
        if self.filename == '':
            raise SystemExit(f'ERROR: 配置文件编号未定义{file_type}！')

        full_name = self.get_config_file()
        if not Path(full_name).is_file():
            raise SystemExit(f'ERROR: {full_name}文件不存在！')

        self.conf = configparser.ConfigParser()  # conf读取配置文件:务必指定编码方式，否则windows下默认以gbk读取utf-8格式的配置文件将会报错
        if platform.system() == "Windows":
            self.conf.read(full_name, encoding="gbk")
        else:
            self.conf.read(full_name, encoding="utf-8")

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        # print(f'type:{exc_type}')
        # print(f'value:{exc_value}')
        # print(f'trace:{traceback}')
        pass

    def get_config_file(self):  # 读取配置文件路径
        local_path = os.path.dirname(os.path.realpath(__file__))  # 实例化conf对象
        if platform.system() == "Windows":
            conf_file = local_path + chr(92) + self.filename
        else:
            conf_file = local_path + "/" + self.filename
        return conf_file

    def get_item(self, section, item_name):  # 读取配置文件section下的一个item
        value = self.conf.get(section, item_name)
        return value

    def get_items(self, section):  # 读取配置文件section下的所有item，dic['work_dir']取值
        dic = dict(self.conf.items(section))
        return dic

    def display_conf(self):  # 显示配置文件的内容
        print('==========================conf.sections()==========================')
        sections = self.conf.sections()
        print('sections:', sections)

        print('\n==========================conf.options(section)==========================')
        for section in sections:
            options = self.conf.options(section)
            print(section, ":", options)

        print('\n==========================conf.items(section)==========================')
        for section in sections:
            option_item = self.conf.items(section)
            print(section, ':', option_item)

        print('\n==========================conf.items优雅之处==========================')
        db_dict = dict(self.conf.items(sections[0]))
        print(db_dict)
        env_dict = dict(self.conf.items(sections[1]))
        print(env_dict)

        print('\n==========================conf.get(section, option)==========================')
        str_val = self.conf.get("bigdata_db", "host")
        print(str_val)
        # print('\n==========================配置文件写操作==========================')
        # self.conf['DEFAULT'] = {'ServerAliveInterval': '45', 'Compression': 'yes', 'CompressionLevel': '9'}
        # 写入配置文件，但所有的注释将不存在
        # with open(self.get_config_file(), "w+", encoding="utf-8") as f:
        #     conf.write(f)


if __name__=='__main__':

    sys = configure_file()
    # print(sys.get_config_file())
    # sys.get_env_var()
    # print(sys.work_dir)
    dic = sys.get_items('environment variables')
    print(dic['work_dir'], dic['log_dir'])
    # sys.display_conf()