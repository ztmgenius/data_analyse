#!/usr/local/bin/python3
"""
根据订单实时自动为导购添加微信好友

https://blog.csdn.net/ma524654165/article/details/77686526/?utm_term=python%E8%87%AA%E5%8A%A8%E5%8C%96%E6%B5%8B%E8%AF%95windows%E7%A8%8B%E5%BA%8F&utm_medium=distribute.pc_aggpage_search_result.none-task-blog-2~all~sobaiduweb~default-5-77686526&spm=3001.4430
https://www.cnblogs.com/dhcn/p/11128251.html
https://www.cnblogs.com/Yinkaisheng/p/3444132.html
https://github.com/yinkaisheng/Python-UIAutomation-for-Windows
pyinstaller -F -w auto_invite_friend.py

按unionid匹配：

select y.date 日期,case when y.mid=2 then '长春新天地' when y.mid=3 then '中东大市场' when y.mid=5 then '中东瑞家' when y.mid=7 then '通化新天地' when y.mid=9 then '吉林新生活'
when y.mid=11 then '松原新天地' when y.mid=18 then '中东福万家超市' when y.mid=19 then '中东砂之船' when y.mid=21 then '宽城中东港' end as 商场,y.sname 店铺名称,y.name 邀请人,y.total 总邀请人数,y.sendsecc 发送成功人数,y.sendfail 发送失败人数,
round(y.sendsecc*100.0/y.total,1) 发送成功率,y.invited 邀请成功人数, round(y.invited*100.0/y.total,1) 邀请成功率,y.mid 商场id ,y.sid 店铺id,y.aid 手机号
from (
select to_char(x.ctime,'yyyy-mm-dd') as date,x.mid,x.sid,x.sname,x.aid,x.name,count(*) total,
sum(case when x.status=1 then 1 else 0 end) sendsecc,
sum(case when x.status=-1 then 1 else 0 end) as sendfail,
sum(case when x.user_id is null then 0 else 1 end) invited
from
(
select d.*,e.userid as user_id  from (
select a.mid,a.sid,a.aid,a.gid,case when a.status=1 then 1 else -1 end as status,a.ctime,b.sname,h.union_id,c.userid,c.name
from wx_work.invite_friend a ,public.ods_shop b,public.dwd_user_info h,wx_work.ods_department_user c
where a.sid=b.id and to_char(a.ctime,'yyyy-mm-dd')>=to_char(now()+'-7 day','yyyy-mm-dd') and a.gid = h.gid
			and c.mobile>'1' and c.mobile<'2' and a.aid=c.mobile::int8
) d
left JOIN wx_work.ods_user_customer e on e.userid=d.userid and e.unionid=d.union_id
) x
group by to_char(x.ctime,'yyyy-mm-dd'),x.mid,x.sid,x.sname,x.aid,x.name
order by to_char(x.ctime,'yyyy-mm-dd') desc ,x.aid,x.name,x.mid,x.sid,x.sname
) y

按昵称匹配：

select y.date 日期,case when y.mid=2 then '长春新天地' when y.mid=3 then '中东大市场' when y.mid=5 then '中东瑞家' when y.mid=7 then '通化新天地' when y.mid=9 then '吉林新生活'
when y.mid=11 then '松原新天地' when y.mid=18 then '中东福万家超市' when y.mid=19 then '中东砂之船' when y.mid=21 then '宽城中东港' end as 商场,y.sname 店铺名称,y.name 邀请人,y.total 总邀请人数,y.sendsecc 发送成功人数,y.sendfail 发送失败人数,
round(y.sendsecc*100.0/y.total,1) 发送成功率,y.invited 邀请成功人数, round(y.invited*100.0/y.total,1) 邀请成功率,y.mid 商场id ,y.sid 店铺id,y.aid 手机号
from (
select to_char(x.ctime,'yyyy-mm-dd') as date,x.mid,x.sid,x.sname,x.aid,x.name,count(*) total,
sum(case when x.status=1 then 1 else 0 end) sendsecc,
sum(case when x.status=-1 then 1 else 0 end) as sendfail,
sum(case when x.user_id is null then 0 else 1 end) invited
from
(
select d.*,e.userid as user_id  from (
select a.mid,a.sid,a.aid,a.gid,a.nickname,case when a.status=1 then 1 else -1 end as status,a.ctime,b.sname,c.userid,c.name
from wx_work.invite_friend a ,public.ods_shop b,wx_work.ods_department_user c
where a.sid=b.id and to_char(a.ctime,'yyyy-mm-dd')>=to_char(now()+'-7 day','yyyy-mm-dd')
			and c.mobile>'1' and c.mobile<'2' and a.aid=c.mobile::int8
) d
left JOIN (select userid,name from wx_work.ods_user_customer where add_way=2 group by userid,name) e on e.userid=d.userid and e.name=d.nickname
) x
group by to_char(x.ctime,'yyyy-mm-dd'),x.mid,x.sid,x.sname,x.aid,x.name
order by to_char(x.ctime,'yyyy-mm-dd') desc ,x.aid,x.name,x.mid,x.sid,x.sname
) y
zxc123.0
"""

import subprocess
import uiautomation as automation
import time
import datetime
import psycopg2
import pandas as pd
import logging.handlers
# import argparse
import os
import configparser
import sys
from dateutil.relativedelta import relativedelta
# from decimal import Decimal
# from cryptography.fernet import Fernet
from Crypto.Cipher import DES
import base64
import requests
import json
import socket


def get_ip_address():
    myname = ' '
    myaddr = ' '
    try:  # 获取本机电脑名
        myname = socket.getfqdn(socket.gethostname())
        #获取本机ip
        myaddr = socket.gethostbyname(myname)
    except:
        logger.info('无法获取本机ip')
        pass

    return myaddr


work_dir = ''
log_dir = ''
config_file = r'\auto_invite_friend.ini'

def get_cur_dir():  # 读取当前目录
    global work_dir, log_dir
    local_path = os.path.dirname(os.path.realpath(__file__))  # 实例化conf对象
    exeruningpath = os.path.dirname(sys.executable)
    if os.path.exists(exeruningpath+config_file):
        work_dir = exeruningpath
    else:
        work_dir = local_path
    log_dir = work_dir + r'\\'


def get_sys_var(section, var_name):  # 读取单个系统变量
    conf = configparser.ConfigParser()  # conf读取配置文件:务必指定编码方式，否则windows下默认以gbk读取utf-8格式的配置文件将会报错
    conf.read(work_dir + config_file, encoding="gbk")
    value = conf.get(section, var_name)
    return value


class server(object):
    def __init__(self):
        self.key = '85247136'
        self.des = DES.new(bytes(self.key, encoding='utf-8'), DES.MODE_ECB)

    def pad(self, s):
        while len(s) % 8 != 0:
            s += ' '
        return s

    def encrypt(self, s):
        s = self.pad(s)
        text = self.des.encrypt(bytes(s, encoding='utf-8'))
        text = base64.standard_b64encode(text)
        return str(text, 'utf-8')

    def decrypt(self, s):
        s = base64.standard_b64decode(s)
        s = self.des.decrypt(s)
        s = str(s, 'utf-8').strip()
        return s

    def db_server_encrypt(self):
        host = self.encrypt('172.16.17.200')
        port = self.encrypt('5432')
        user = self.encrypt('postgres    ')
        password = self.encrypt('    ')
        database = self.encrypt(' postgres')
        print(f'encrypt : \nhost= {host}\nport= {port}\nuser= {user}\npassword= {password}\ndatabase= {database}')
        # 写入配置文件，但所有的注释将不存在
        # conf = configparser.ConfigParser()  # conf读取配置文件:务必指定编码方式，否则windows下默认以gbk读取utf-8格式的配置文件将会报错
        # conf.read(work_dir + config_file, encoding="utf-8")
        # conf['server'] = {'host': host, 'port': port, 'user': user, 'password': password, 'database': database}
        # print(conf)
        # with open(work_dir + config_file, "w+", encoding="utf-8") as f:
        #     conf.write(f)

    def db_server_decrypt(self):
        host = get_sys_var("server", "host")  #
        port = get_sys_var("server", "port")
        user = get_sys_var("server", "user")
        password = get_sys_var("server", "password")
        database = get_sys_var("server", "database")
        self.host = self.decrypt(host)
        self.port = self.decrypt(port)
        self.user = self.decrypt(user)
        self.password = self.decrypt(password)
        self.database = self.decrypt(database)
        # logger.info(f'\nconnect database: \n\thost= {self.host}\n\tport= {self.port}\n\tuser= {self.user}\n\tpassword= {self.password}\n\tdatabase= {self.database}')


get_cur_dir()


def get_config():
    global friends, last_days, recent_days, rejects, groups, days,delay ,max_friends ,worker
    global sending ,demo ,phone, invited_msg, open_hours, open_hours, filename, WXWork_exe, admin, new_y, linker_y
    friends    = int(get_sys_var("rules", "friends"))  # 添加人人数小于n
    last_days  = int(get_sys_var("rules", "last_days"))  # 最近n天不重复加
    recent_days= int(get_sys_var("rules", "recent_days"))  # 拒绝次数大于n
    rejects    = int(get_sys_var("rules", "rejects"))  # 拒绝次数大于n
    groups     = int(get_sys_var("rules", "groups"))  # 入群数大于n
    days       = int(get_sys_var("rules", "days"))  # 是否添加员工为好友，1添加，0不添加
    delay      = int(get_sys_var("rules", "delay"))  # 添加好友间隔秒数,避免封号
    max_friends= int(get_sys_var("rules", "max_friends"))  # # 最多好友数
    worker     = int(get_sys_var("rules", "worker"))  # 是否添加员工为好友，1添加，0不添加
    sending    = int(get_sys_var("environment variables", "sending"))  # 是否直接发送邀请，1发送，0不发送
    demo       = int(get_sys_var("environment variables", "demo"))  # 演示模式，1模拟演示，0正式运行
    phone      = int(get_sys_var("environment variables", "phone"))  # 导购手机号
    invited_msg= get_sys_var("environment variables", "invited_msg")  # 邀请欢迎语，{}代表店铺名称
    open_hours = get_sys_var("environment variables", "open_hours")  # 营业时间
    filename   = get_sys_var("environment variables", "filename")  # 程序当前目录的指定文件导入电话号码，必须是每个电话号码一行的文本文件，No：不从文件导入
    WXWork_exe = get_sys_var("environment variables", "WXWork_exe")  # 企业微信程序 C:\Program Files (x86)\WXWork\WXWork.exe
    admin      = get_sys_var("environment variables", "admin")  # 系统管理员，企业微信账号
    new_y      = int(get_sys_var("environment variables", "new_y"))  # 新的客户y轴
    linker_y   = int(get_sys_var("environment variables", "linker_y"))  # 新的客户y轴

    open_hours = open_hours.split('-')  # 规范时间格式
    open_hours[0] = ':'.join([str(int(i)).zfill(2) for i in open_hours[0].split(':')]).strip()
    open_hours[1] = ':'.join([str(int(i)).zfill(2) for i in open_hours[1].split(':')]).strip()


get_config()

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
    while 1:
        try:
            conn = psycopg2.connect(host=db_server.host, port=db_server.port, user=db_server.user,
                                password=db_server.password, database=db_server.database)
            break
        except Exception as e:
            logger.error(f"ERROR: 连接服务器出错:{e}")
            time.sleep(60)
    try:
        data = pd.read_sql(sql_command, conn)  # 有列名
    except Exception as e:
        logger.error(f"ERROR: select_sql执行出错:{sql_command} , {e}")
    conn.close()
    return data


def execute_sql(sql_command):
    sql_command = sql_command.strip()
    while 1:
        try:
            conn = psycopg2.connect(host=db_server.host, port=db_server.port, user=db_server.user,
                                    password=db_server.password, database=db_server.database)
            break
        except Exception as e:
            logger.error(f"ERROR: 连接服务器出错:{e}")
            time.sleep(60)

    cur = conn.cursor()
    try:
        cur.execute(sql_command)
    except Exception as e:
        try:
            logger.error(f"ERROR: execute_sql执行sql出错:{sql_command} , {e}")
        except:
            logger.error("ERROR: execute_sql执行sql出错:可能是insert语句带有特殊字符导致")
        conn.close()
        return 1
    if sql_command[0:6].lower() == 'select':
        data = pd.DataFrame(cur.fetchall())  # 无列名
        conn.close()
        return data
    conn.commit()
    conn.close()


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

corpid = 'wx7d8e879ec36eb6c9'
Umall_Secret = "B13BcfNIYiBpZip7pcRw2PuFjLzkc1LocfESnCqQ9AE"  # 优mall
Umall_agentid = 1000011
time_out = 3  # 超时设置，秒数
retry_times = 5  # 设置失败的请求尝试次数
URL = 'https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={}&corpsecret={}'

def get_token(corp_id, secret):  # 封装获取access_token
    retrys = 1
    resp = ''
    while retrys <= retry_times:
        try:
            resp = requests.get(url=URL.format(corp_id, secret))
        except Exception as e:
            if retrys == retry_times:
                logger.error("get_token无法获取token, retrys={0}, pid={1}, ppid={2}".format(retrys, os.getpid(), os.getppid()))
                raise SystemExit('ERROR: get_token无法获取token, corp_id:{0} , secret:{1}'.format(corp_id, secret))
            time.sleep(10)
        else:
            break
        logger.debug('retrys={0},{1}'.format(retrys, resp))
        retrys += 1
    try:
        access_token = resp.json().get('access_token')
    except Exception as e:
        logger.error("error={0}\t{1}".format(e, resp))
        return None
    return access_token


def send_message_to_user(content, user_list):  # 给用户发送文本消息
    access_token = get_token(corpid, Umall_Secret)
    content = time.strftime('%Y-%m-%d %X', time.localtime()) + ' 主机(' + user_name + ' ' + mobile + ' ' + get_ip_address() + ') ' + content
    data = {
       "touser": user_list,   # 向这些用户账户发送
       # "toparty" : "PartyID1|PartyID2",   # 向这些部门发送
       "msgtype": "text",
       "agentid": Umall_agentid,                       # 应用的 id 号
       "text": {
           "content": content
       },
       "safe": 0
    }
    try:
        r = requests.post(url="https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={}".format(access_token),
                  data=json.dumps(data))
    except Exception as e:
        logger.error("向用户发送文本消息出错:{0}".format(e))


invited_error = [[-20, "该手机号未绑定微信"],
                 [-21, "内部员工的企微号"],
                 [-22, "该客户已添加过好友"],
                 [-23, "模拟发送邀请"],
                 [-24, "该手机号员工已离职"],
                 [-1, "非营业时间"],
                 [-2, "已经是好友"],
                 [-3, "已被N个导购加为好友"],
                 [-4, "最近N天内不能重复添加"],
                 [-5, "在最近N天内已拒绝M次"],
                 [-6, "加入了N个群"],
                 [-7, "场内工作人员"],
                 [-100, "系统错误"],
                 [-101, "封禁提示：添加过于频繁"],
                 [-110, "未定义错误"],
                 [1, "已发送邀请"]]

def get_err_msg(code):
    try:
        return [x[1] for x in invited_error if x[0] == code][0]
    except:
        return "未定义错误"


prohibited = 0  # 0：解封，1：封禁
def invite_friend(gid, msg):
    global prohibited
    nickname = ''
    try:
        # print(automation.GetRootControl())
        subprocess.Popen(WXWork_exe)
        time.sleep(0.5)
        WXWorkWindow = automation.WindowControl(searchDepth=1, ClassName='WeWorkWindow')
        WXWorkWindow.Maximize()
        time.sleep(0.5)
        WXWorkWindow.SetTopmost(False)
        WXWorkWindow.Click(23, linker_y)  # x 左上角横向，y 左上角纵向 企业微信-通讯录, simulateMove=False
        time.sleep(0.2)
        WXWorkWindow.Click(180, new_y)  # x 企业微信-通讯录-新的客户, simulateMove=False
        time.sleep(0.3)
        WXWorkWindow.Click(WXWorkWindow.BoundingRectangle.width()-75, 35)  # x 企业微信-通讯录-新的客户-添加

        time.sleep(1)
        searchWindow = automation.WindowControl(searchDepth=2, ClassName='SearchExternalsWnd')  # 打开添加窗口
        time.sleep(1)
        x = int(searchWindow.BoundingRectangle.width() / 2)
        y = int(searchWindow.BoundingRectangle.height() / 2)

        edit = searchWindow.PaneControl()
        edit.SendKeys(gid)  # 输入电话
        searchWindow.SendKeys('{ENTER}')  # 搜索
        time.sleep(2)

        noUserWindow = automation.WindowControl(searchDepth=2, ClassName='WeWorkMessageBoxFrame')
        if noUserWindow.Exists(maxSearchSeconds=2) == True:
            if noUserWindow.Name == '该用户不存在':
                logger.warning(f"该手机号未绑定微信：{gid}")
                noUserWindow.SendKeys('{ESC}{ESC}')  # 关闭添加
                WXWorkWindow.SendKeys('{ESC}{ESC}')
                return -20, ''
            else:
                logger.error(f"搜索手机号发生未知错误：{gid}")
                # WXWorkWindow.CaptureToImage(f"""{str(gid)}-{datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.png""")  # 保留证据
                noUserWindow.SendKeys('{ESC}{ESC}')  # 关闭添加
                WXWorkWindow.SendKeys('{ESC}{ESC}')
                return -110, '搜索手机号发生未知错误'

        # searchWindow.SetFocus()
        # searchWindow.Click(int(x * 2 * 3 / 4) - 50, int(y * 2 * 5 / 9) -3)  # 点击用户信息,取昵称
        # time.sleep(0.5)
        # UserInfoWindow = automation.PaneControl(searchDepth=2, ClassName='UserInfoWindow')
        # automation.SetClipboardText('')
        # UserInfoWindow.RightClick(18, 85)
        # UserInfoWindow.SendKeys('{DOWN}{ENTER}{ESC}')
        # nickname = automation.GetClipboardText().strip()
        # if nickname == '':  # 企业内部员工的企微号不添加
        #     logger.warning(f"内部员工的企微号：{gid}")
        #     searchWindow.SendKeys('{ESC}{ESC}')
        #     return -21, ''

        # 修改描述
        # searchWindow.SetFocus()
        # searchWindow.Click(int(x * 2 * 3 / 4) - 50, int(y * 2 * 5 / 9) - 3)  # 点击用户信息
        # UserInfoWindow = automation.PaneControl(searchDepth=2, ClassName='UserInfoWindow')
        # automation.SetClipboardText('')
        # UserInfoWindow.RightClick(18, 115)
        # UserInfoWindow.SendKeys('{DOWN}{ENTER}')
        # nickname1 = ''
        # nickname1 = automation.GetClipboardText()[5:].strip()
        # if nickname1 != '':
        #     UserInfoWindow.Click(18, 160)
        #     nickname = nickname1
        # else:
        #     UserInfoWindow.Click(18, 140)
        # ModifyUserContactGroupWindow = automation.PaneControl(searchDepth=3, ClassName='ModifyUserContactGroupWindow')
        # if ModifyUserContactGroupWindow.Exists(maxSearchSeconds=1) == True:
        #     ModifyUserContactGroupWindow.Click(50, int(ModifyUserContactGroupWindow.BoundingRectangle.height() * 3 / 4))
        #     ModifyUserContactGroupWindow.SendKeys('{Ctrl}a{del}')
        #     ModifyUserContactGroupWindow.SendKeys('auto-add{ENTER}')
        #     time.sleep(2)
        # UserInfoWindow.SendKeys('{ESC}')
        # time.sleep(0.5)

        time.sleep(0.5)
        searchWindow.SetFocus()
        searchWindow.Click(int(x*2*3/4), int(y*2*5/9))  # 点击添加, simulateMove=False
        UserInfoWindow = automation.PaneControl(searchDepth=2, ClassName='UserInfoWindow')
        if UserInfoWindow.Exists(maxSearchSeconds=2) == True:  # 企业内部员工的企微号不添加, 企微号无添加按钮
            logger.warning(f"内部员工的企微号：{gid}")
            searchWindow.SendKeys('{ESC}{ESC}')
            WXWorkWindow.SendKeys('{ESC}{ESC}')
            return -21, ''

        time.sleep(2)
        prohibitWindow = automation.WindowControl(searchDepth=2, ClassName='WeWorkMessageBoxFrame')
        if prohibitWindow.Exists(maxSearchSeconds=3) == True:
            title_name = prohibitWindow.Name.strip()
            logger.info(f'弹出窗口1：{title_name}')
            # WXWorkWindow.CaptureToImage(f"""{str(gid)}-{datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.png""")  # 保留证据
            if title_name == '封禁提示':
                prohibitWindow.SendKeys('{ESC}{ESC}')  # 关闭添加
                searchWindow.SendKeys('{ESC}')  # 关闭添加
                logger.error(f"封禁提示1：添加好友过于频繁：{gid}")
                send_message_to_user('封禁提示1：添加好友过于频繁，12小时后，系统自动重试', admin)
                if prohibited == 0:
                    time.sleep(12 * 60 * 60)
                    prohibited = 1
                else:
                    time.sleep(1 * 60 * 60)
                # send_message_to_user('封禁已达12小时，系统自动重试，请注意观察；若未解封系统将自动再延长12小时后重试，你可以在进入系统休眠期间进行手动重新启动系统。', admin)
                return -101, ''
                # sys.exit(-100)
            else:
                logger.error(f"该企微号已离职或其它原因导致，不能添加1：{gid}")
                prohibitWindow.SendKeys('{ESC}{ESC}')  # 关闭添加
                searchWindow.SendKeys('{ESC}')  # 关闭添加
                WXWorkWindow.SendKeys('{ESC}{ESC}')
                return -24, nickname

        time.sleep(0.5)
        searchWindow.SetFocus()
        searchWindow.Click(x, y - 25, simulateMove=False)  # 获得编辑光标
        edit = searchWindow.EditControl(searchDepth=3, ClassName='EditWnd')
        time.sleep(0.5)

        if edit.Exists(maxSearchSeconds=2) == False:
            # noUserWindow.Name = '该用户已添加'
            logger.warning(f"该客户已添加过好友：{gid}")
            searchWindow.SendKeys('{ESC}')  # 关闭添加
            WXWorkWindow.SendKeys('{ESC}{ESC}')
            return -22, nickname

        edit.SendKeys('{Ctrl}a{del}')
        edit.SendKeys(msg)  # 添加好友理由
        """
        
2021-12-28 18:07:03.919 automation.py[79] main -> Starts, Current Cursor Position: (1010, 454)
ControlType: PaneControl    ClassName: #32769    AutomationId:     Rect: (0,0,1920,1080)[1920x1080]    Name: '桌面'    Handle: 0x10010(65552)    Depth: 0    SupportedPattern: LegacyIAccessiblePattern
    ControlType: WindowControl    ClassName: SearchExternalsWnd    AutomationId:     Rect: (765,340,1155,700)[390x360]    Name: '添加客户'    Handle: 0xD07C2(853954)    Depth: 1    SupportedPattern: LegacyIAccessiblePattern TransformPattern WindowPattern
        ControlType: WindowControl    ClassName: WeWorkMessageBoxFrame    AutomationId:     Rect: (780,433,1140,592)[360x159]    Name: '提示'    Handle: 0x150204(1376772)    Depth: 2    SupportedPattern: LegacyIAccessiblePattern WindowPattern
            ControlType: PaneControl    ClassName: PerryShadowWnd    AutomationId:     Rect: (768,421,1152,604)[384x183]    Name: ''    Handle: 0x13025C(1245788)    Depth: 3    SupportedPattern: LegacyIAccessiblePattern TransformPattern
        ControlType: PaneControl    ClassName: PerryShadowWnd    AutomationId:     Rect: (753,328,1167,712)[414x384]    Name: ''    Handle: 0xF0612(984594)    Depth: 2    SupportedPattern: LegacyIAccessiblePattern TransformPattern
2021-12-28 18:07:03.980 automation.py[114] main -> Ends

ControlType: PaneControl    ClassName: #32769    AutomationId:     Rect: (0,0,1920,1080)[1920x1080]    Name: '桌面'    Handle: 0x10010(65552)    Depth: 0    SupportedPattern: LegacyIAccessiblePattern
    ControlType: WindowControl    ClassName: WeWorkWindow    AutomationId:     Rect: (-2,-2,1922,1042)[1924x1044]    Name: '企业微信'    Handle: 0x200648(2098760)    Depth: 1    SupportedPattern: LegacyIAccessiblePattern TransformPattern WindowPattern
        ControlType: WindowControl    ClassName: WeWorkMessageBoxFrame    AutomationId:     Rect: (793,361,1153,520)[360x159]    Name: '封禁提示'    Handle: 0x110276(1114742)    Depth: 2    SupportedPattern: LegacyIAccessiblePattern WindowPattern
            ControlType: PaneControl    ClassName: PerryShadowWnd    AutomationId:     Rect: (781,349,1165,532)[384x183]    Name: ''    Handle: 0x1307C4(1247172)    Depth: 3    SupportedPattern: LegacyIAccessiblePattern TransformPattern
        ControlType: PaneControl    ClassName: TitleBarWindow    AutomationId:     Rect: (-2,-2,1922,26)[1924x28]    Name: ''    Handle: 0x100758(1050456)    Depth: 2    SupportedPattern: LegacyIAccessiblePattern
        ControlType: PaneControl    ClassName: TransparentTitleBarWindow    AutomationId:     Rect: (-2,-1,1922,27)[1924x28]    Name: ''    Handle: 0xA05CC(656844)    Depth: 2    SupportedPattern: LegacyIAccessiblePattern
        ControlType: PaneControl    ClassName: PerryShadowWnd    AutomationId:     Rect: (-14,-14,1934,1054)[1948x1068]    Name: ''    Handle: 0x110610(1115664)    Depth: 2    SupportedPattern: LegacyIAccessiblePattern TransformPattern
        ControlType: PaneControl    ClassName: WXworkWindow - 企业微信-通讯录    AutomationId:     Rect: (57,-2,1922,1042)[1865x1044]    Name: '企业微信-通讯录'    Handle: 0x5029E(328350)    Depth: 2    SupportedPattern: LegacyIAccessiblePattern TransformPattern

        """
        if demo == 0:  # 正式运行
            if sending == 1:  # 点击发送
                time.sleep(0.5)
                searchWindow.Click(x, y + 40)  # 发送
                time.sleep(3)
                # input('input:')
                prohibitWindow = automation.WindowControl(searchDepth=2, ClassName='WeWorkMessageBoxFrame')
                if prohibitWindow.Exists(maxSearchSeconds=3) == True:
                    title_name = prohibitWindow.Name.strip()
                    logger.info(f'弹出窗口2：{title_name}')
                    # WXWorkWindow.CaptureToImage(f"""{str(gid)}-{datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.png""")  # 保留证据
                    if title_name == '封禁提示':
                        prohibitWindow.SendKeys('{ESC}{ESC}')  # 关闭添加
                        searchWindow.SendKeys('{ESC}')  # 关闭添加
                        logger.error(f"封禁提示2：添加好友过于频繁2：{gid}")
                        send_message_to_user('封禁提示2：添加好友过于频繁，12小时后，系统自动重试', admin)
                        if prohibited == 0:
                            time.sleep(12 * 60 * 60)
                            prohibited = 1
                        else:
                            time.sleep(1 * 60 * 60)
                        # send_message_to_user('封禁已达12小时，系统自动重试，请注意观察；若未解封系统将自动再延长12小时后重试，你可以在进入系统休眠期间进行手动重新启动系统。', admin)
                        return -101, ''
                        # sys.exit(-100)
                    else:
                        logger.error(f"该企微号已离职或其它原因导致，不能添加2：{gid}")
                        prohibitWindow.SendKeys('{ESC}{ESC}')  # 关闭添加
                        searchWindow.SendKeys('{ESC}')  # 关闭添加
                        WXWorkWindow.SendKeys('{ESC}{ESC}')
                        return -24, nickname
            else:
                searchWindow.SendKeys('{ESC}')  # 关闭添加
                WXWorkWindow.SendKeys('{ESC}{ESC}')
                return -23, nickname
        else:
            pass
        searchWindow.SendKeys('{ESC}')  # 关闭添加
    except Exception as e:
        logger.error(f"invite_friend : {gid}，{e}")
        # WXWorkWindow.CaptureToImage(f"""{str(gid)}-{datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.png""")  # 保留证据
        WXWorkWindow.SendKeys('{ESC}{ESC}')
        return -100, f"自动添加好友系统出错了，请登陆服务器查看日志，{gid}，{e}"
    WXWorkWindow.SendKeys('{ESC}{ESC}')
    return 1, nickname


def get_userid(phone):
    userid = select_sql(f"""select userid,case when mid is null then -1 else mid end as mid from wx_work.ods_department_user where mobile='{phone}'""")
    if userid is None or len(userid) == 0:
        logger.error(f"该企微用户不存在,mobile：{phone}")
        sys.exit(-1)
    else:
        return userid['userid'][0], userid['mid'][0]


def do_not_disturb(gid, mid, sid):
    """
    规则：
        # 已添加导购人数小于friends
        friends = 10
        # last_days天内不重复加
        last_days = 7
        # 最近recent_days天内，拒绝rejects次的顾客，不添加
        recent_days = 30
        rejects = 3
        # 入群数大于groups，不添加
        groups = 3
        # 只添加最近days天消费的顾客，0当天，1昨日，2前日，...
        days = 0
        # 添加好友间隔秒数,避免封号
        delay = 0
        # 最多好友数
        max_friends = 0
        # 是否添加场内人员为好友，1添加，0不添加
        worker = 0
    :param gid: 客户手机号
    :return:
    """
    user = select_sql(f"""select gid,union_id from public.dwd_user_info where gid={gid}""")  # 查询顾客union_id
    if user is None or len(user) == 0:
        logger.info(f"mid={mid}\t新客：{gid}")
        return 0
    unionid = user['union_id'][0]  # 顾客union_id

    # 查询顾客的添加人
    user_id = select_sql(f"""select userid from wx_work.ods_user_customer where unionid='{unionid}' and mid={mid} """)
    workers = len(user_id)
    if workers > 0:
        user_list = user_id.userid.values.tolist()
        if qw_userid in user_list:
            logger.info(f"mid={mid}\t{phone}已经是{gid}好友，不再添加")
            return -2
        if workers > friends:
            logger.info(f"mid={mid}\t{gid}已被{workers}个导购加为好友，不再添加")
            return -3

    # 查询邀请次数及最近邀请时间
    cur_date = (datetime.datetime.now() - relativedelta(days=recent_days+1)).strftime('%Y-%m-%d')
    invited = select_sql(f"""select count(*) as cnt,max(ctime) as last_time from wx_work.invite_friend 
                            where gid={gid} and status=1 and aid={phone} and sid={sid} and to_char(ctime,'yyyy-mm-dd')>'{cur_date}'
                            group by to_char(ctime,'yyyy-mm-dd'),aid
                            order by to_char(ctime,'yyyy-mm-dd') desc,aid""")
    times = len(invited)
    if times > 0:
        if int(time.time()) - int(invited.last_time[0].value / 1000/1000/1000) > last_days * 24 * 3600:
            logger.info(f"mid={mid} sid={sid}\t最近{last_days}天内不能重复添加{gid}")
            return -4
        if times > rejects:
            logger.info(f"mid={mid} sid={sid}\t{gid}在最近{recent_days}天内已拒绝{times}次，不再添加")
            return -5

    # 查询所属的群主
    group = select_sql(f"""select distinct b.owner from wx_work.ods_groupchat_customer a,wx_work.ods_groupchat_info b
            where a.unionid='{unionid}' and a.chat_id=b.chat_id and a.mid={mid} """)
    if len(group) > groups:
        logger.info(f"""mid={mid}\t加入了{len(group)}个群，不再添加。{group.owner.values.tolist()}""")
        return -6

    # 查询是否员工
    if mid > 2:
        isworker = select_sql(f"""select worker from mall{mid}.dwa_user_rfm where gid={gid}""")
        if isworker is not None and len(isworker) > 0 and isworker['worker'][0] == 1:
            if worker == 1:
                return 0
            else:
                logger.info(f"""mid={mid}\t{gid}是场内工作人员，不再添加。""")
                return -7
    return 0


notice_times = 0  # 发生系统性错误时，最多通知次数
def main(phone):
    """
    :param delay: 每次添加好友间隔
    :param phone:导购手机号
    :return:
    """
    global notice_times
    global prohibited
    while 1:
        get_config()
        otime = datetime.datetime.now().strftime("%H:%M:%S")
        if otime < open_hours[0] or otime > open_hours[1]:  # 非营业时间
            time.sleep(60)
            logger.warning("非营业时间")
            notice_times = 0
            continue

        sql = f"select b.sid,a.sname ,b.phone from public.ods_shop a, wx_work.ods_shop_qw_owner b where a.id=b.sid and b.phone={phone} and status=1"
        # sql = f"select a.id as sid,a.sname ,'{phone}' as phone from public.ods_shop a where a.id in (10940,8773,10927,13243,8145,12085,12623,25433,12324,16033)"
        data = select_sql(sql)
        if data is None or len(data) == 0:
            logger.warning(f"导购不存在wx_work.ods_shop_qw_owner：{phone}")
            return

        sql = f"select count(*) from wx_work.ods_user_customer where userid='{qw_userid}'"
        frds = select_sql(sql)
        if frds['count'][0] >= max_friends:
            msg = f"{phone} 好友人数已达{frds['count'][0]}人，即将达到上限，不能再添加，请联系系统管理员。"
            logger.error(msg)
            send_message_to_user(msg, qw_userid)
            return

        cur_date = (datetime.datetime.now() - relativedelta(days=days)).strftime('%Y-%m-%d')
        sid_list ="("+','.join(data.sid.astype(str).tolist())+")"
        sql = f"""select gid,order_no::text,mid,sid from wx_work.ods_order_info where sid in {sid_list} and send_status=0 and to_char(ctime,'yyyy-mm-dd')='{cur_date}'
                    and gid not in (select distinct gid from wx_work.ods_order_info where sid in {sid_list} and send_status<>0 and to_char(ctime,'yyyy-mm-dd')='{cur_date}')
                    order by ctime desc
                    limit 1
                """  # 查询当日订单，每个客户只添加一次

        order_info = select_sql(sql)
        if order_info is None or len(order_info) == 0:
            logger.debug(f"所属店铺无消费记录，{sid_list}")
            time.sleep(60)
            continue

        # order_info['order_no'] = order_info['order_no'].apply(lambda x: Decimal(x))
        gid = str(order_info['gid'][0])
        order_no = int(order_info['order_no'][0])
        mid = order_info['mid'][0]
        sid = order_info['sid'][0]
        sname = list(data[data['sid'] == sid]['sname'])[0]

        msg = invited_msg.format(sname)
        status = do_not_disturb(gid, mid, sid)  # 免打扰
        nickname = ''
        if status == 0:  # 符合邀请规则
            status, nickname = invite_friend(gid, msg)
            if status == -100:
                err_msg = '系统错误'
                logger.error(f"mid:{mid}, sid:{sid}, sname:{sname}, gid:{gid}, order_no:{order_no}, status:{status}, err_msg= {err_msg}")
                if notice_times < 3:
                    if notice_times >= 0:
                        send_message_to_user(f'第{notice_times}次，{nickname}', admin)
                    notice_times += 1
                time.sleep(60*1)
                continue
            if status == -101:
                continue
        notice_times = 0
        err_msg = get_err_msg(status)
        nickname = nickname.replace('\'', '\'\'')  # 转义
        logger.info(f"mid:{mid}, sid:{sid}, sname:{sname}, gid:{gid}, order_no:{order_no}, status:{status}, err_msg= {err_msg}")
        sql = f"""update wx_work.ods_order_info set send_status={status} where gid={gid} and mid={mid} and sid in {sid_list}"""
        execute_sql(sql)
        sql = f"""insert into wx_work.invite_friend(gid,aid,mid,sid,order_no,status,err_msg,nickname,host) 
                                            values({gid},{phone},{mid},{sid},{order_no},{status},'{err_msg}','{nickname}','{get_ip_address()}')"""
        execute_sql(sql)
        if status == 1:
            if prohibited == 1:
                prohibited = 0
                send_message_to_user('已解封^_^', admin)
            time.sleep(delay)


def invite_friend_from_file(phone):
    """
    :param delay: 每次添加好友间隔
    :param phone:导购手机号
    :return:
    """
    otime = datetime.datetime.now().strftime("%H:%M:%S")
    if otime < open_hours[0] or otime > open_hours[1]:  # 非营业时间
        logger.warning("非营业时间，不能添加好友。")
        return

    sql = f"select b.sid,a.sname ,b.phone from public.ods_shop a, wx_work.ods_shop_qw_owner b where a.id=b.sid and b.phone={phone} and status=1"
    # sql = f"select a.id as sid,a.sname ,'{phone}' as phone from public.ods_shop a where a.id in (10940,8773,10927,13243,8145,12085,12623,25433,12324,16033)"
    data = select_sql(sql)
    if data is None or len(data) == 0:
        logger.warning(f"导购不存在wx_work.ods_shop_qw_owner：{phone}")
        return
    sid_list = "(" + ','.join(data.sid.astype(str).tolist()) + ")"

    cur_date = (datetime.datetime.now() - relativedelta(days=days)).strftime('%Y-%m-%d')
    file = work_dir + r'\\' + filename
    with open(file, 'r') as f:
        for line in f.readlines():  # 循环所有要发送的电话
            gid = line.strip()  # 把末尾的'\n'删掉
            if len(gid) != 11:
                if len(gid) > 0:
                    logger.warning(f"非法手机号：{gid}")
                continue

            sql = f"select count(*) from wx_work.ods_user_customer where userid='{qw_userid}'"
            frds = select_sql(sql)
            if frds['count'][0] >= max_friends:
                msg = f"{phone} 导购好友人数已达{frds['count'][0]}人，即将达到上限，不能再添加，请联系系统管理员。"
                logger.error(msg)
                send_message_to_user(msg, qw_userid)
                return

            sql = f"""select gid,order_no::text,mid,sid from wx_work.ods_order_info where sid in {sid_list} 
                        and send_status=0 and gid={gid} and to_char(ctime,'yyyy-mm-dd')>='{cur_date}' 
                        order by ctime desc
                        limit 1
                    """  # 查询历史订单，每个客户只添加一次

            order_info = select_sql(sql)
            if len(order_info) == 0:
                logger.debug(f"{gid}最近无消费记录，不能添加")
                continue

            # order_info['order_no'] = order_info['order_no'].apply(lambda x: Decimal(x))
            gid = str(order_info['gid'][0])
            order_no = int(order_info['order_no'][0])
            mid = order_info['mid'][0]
            sid = order_info['sid'][0]
            sname = list(data[data['sid'] == sid]['sname'])[0]
            msg = invited_msg.format(sname)
            sid = order_info['sid'][0]

            status = do_not_disturb(gid, mid, sid)  # 免打扰
            nickname = ''
            if status == 0:  # 符合邀请规则
                status, nickname = invite_friend(gid, msg)
                if status == -100:
                    err_msg = '系统错误'
                    logger.error(f"mid:{mid}, sid:{sid}, sname:{sname}, gid:{gid}, order_no:{order_no}, status:{status}, err_msg= {err_msg}")
                    time.sleep(60 * 1)
                    continue
            err_msg = get_err_msg(status)
            nickname = nickname.replace('\'', '\'\'')  # 转义
            logger.info(f"mid:{mid}, sid:{sid}, sname:{sname}, gid:{gid}, order_no:{order_no}, status:{status}, err_msg= {err_msg}")
            sql = f"""insert into wx_work.invite_friend(gid,aid,mid,sid,order_no,status,err_msg,nickname) 
                                                values({gid},{phone},{mid},{sid},{order_no},{status},'{err_msg}','{nickname}')"""
            execute_sql(sql)
            if status == 1:
                time.sleep(delay)


def mydemo():
    msg = f'我是您的专属服务人员，任何的问题，随时为您服务。'
    for i in range(1, 8):
        invite_friend('15543777785', msg)  # 已添加过该好友
        invite_friend('13578630865', msg)  # 昵称是一个点 13578630865
        invite_friend('18686668743', msg)  # 昵称有特殊字符
        invite_friend('13756690628', msg)  # 外部的企业微信号 13756690628
        invite_friend('13069193588', msg)  # 该手机号未绑定微信：13069193588
        invite_friend('13664355123', msg)  # 内部员工的企微号：13664355123
        pass


if __name__ == '__main__':
    start_time = datetime.datetime.now()
    logger = bigdata_logger('auto_invite_friend.log')
    logger.info('auto_invite_friend starting ...')

    db_server = server()
    # db_server.db_server_encrypt()  # 生成数据库服务器加密串
    db_server.db_server_decrypt()

    # parser = argparse.ArgumentParser(description='自动添加好友')
    # parser.add_argument('-p', dest="phone", type=int, required=True, help='导购手机号')  # 导购手机号
    # args = parser.parse_args()
    # phone = args.phone

    p = subprocess.Popen(WXWork_exe,
                         shell=True,
                         stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    time.sleep(1)
    if p.returncode != 0:
        logger.info(f'无法调用{WXWork_exe},请检查程序配置文件')
        sys.exit(-1)

    WXWorkWindow = automation.WindowControl(searchDepth=1, ClassName='WeWorkWindow')
    WXWorkWindow.Maximize()

    # file = f"""abc.png"""
    # print(file)
    # WXWorkWindow.CaptureToImage("abc.png")  # 保留证据
    # exit(-1)


    WXWorkWindow.Click(23, linker_y)  # x 左上角横向，y 左上角纵向 企业微信-通讯录
    WXWorkWindow.Click(180, new_y)  # x 企业微信-通讯录-新的客户
    WXWorkWindow.Click(23, 53)  # 获取当前用户手机号
    time.sleep(0.2)
    userInfoWindow = automation.PaneControl(searchDepth=2, ClassName='UserInfoWindow')
    automation.SetClipboardText('')
    userInfoWindow.RightClick(110, 35)
    userInfoWindow.SendKeys('{DOWN}{ENTER}')
    user_name = automation.GetClipboardText().strip()  # 取用户姓名
    automation.SetClipboardText('')
    userInfoWindow.RightClick(117, 162)
    userInfoWindow.SendKeys('{DOWN}{ENTER}{ESC}')
    mobile = automation.GetClipboardText().strip()  # 取用户手机号码，有照片时，位置是否变化待验证
    logger.info(f'当前登录企微用户：{user_name}\t手机号： {mobile}')



    if demo == 1:
        mydemo()
    else:
        qw_userid, mid = get_userid(phone)
        if filename.lower() != 'no':
            invite_friend_from_file(phone)
        else:
            main(phone)

    logger.info("auto_invite_friend Total duration {}".format(datetime.datetime.now() - start_time))

