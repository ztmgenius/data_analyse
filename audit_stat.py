#!/usr/local/bin/python3
"""
审计汇总
"""

import pandas as pd
import datetime
from time import strftime, localtime

import common


logger = ''

class running_duration(object):  # 运行时长装饰类
    def __init__(self, *args):
        self.func = args[0]
    def __call__(self, *args):
        start_time = datetime.datetime.now()
        logger.info("{0} - start time:{1}".format(self.func.__name__, start_time))
        if len(args) == 0:
            self.func()
        elif len(args) == 1:
            self.func(args[0])
        elif len(args) == 2:
            self.func(args[0], args[1])
        end_time = datetime.datetime.now()
        logger.info("{0} - Total duration:{1}".format(self.func.__name__, end_time - start_time))


def unique_field(field_name):
    field = [f.strip() for f in field_name.split(',')]
    field_set = list(set(field))
    field_set.sort()
    return ','.join(field_set)


def field_format(field_value):
    if field_value is None:
        return ''
    else:
        return field_value.strip()


#@running_duration
def insert_audit(data, type):
    """
    type:
        1:  姓名
        2:  手机号
        3:  车牌号
        4:  身份证
        5:  银行卡
        6:  名称
    system_type:
        A:  OA
        B:  停车场
        C:  采购
        D:  嘉码
        E:  金蝶
        F:  企微
        G:  PRM
        H:  优mall店铺
        I:  优mall会员
        ...
    """
    if data is None or len(data) == 0:
        logger.info("数据集为空，不处理")
        return
    data['type'] = type
    data['ctime'] = strftime('%Y-%m-%d %H:%M:%S', localtime())
    data = data[['type', 'field_value', 'system_type', 'field_name', 'ctime']]
    logger.info("data grouping :{}".format(len(data)))
    num_agg = {'field_name': ['count', ','.join]}
    data1 = data.groupby(['type', 'field_value', 'system_type']).agg(num_agg).reset_index()
    data1.columns = ['type', 'field_value', 'system_type', 'fields', 'field_name']
    logger.info("data1: unique field_name running ...")
    data1['field_name'] = data1['field_name'].apply(unique_field)
    data1['ctime'] = strftime('%Y-%m-%d %H:%M:%S', localtime())
    data1 = data1[['type', 'field_value', 'system_type', 'fields', 'field_name', 'ctime']]

    logger.info("data1 grouping :{}".format(len(data1)))
    num_agg = {'system_type': ['count', ''.join], 'fields': ['sum'], 'field_name': [','.join]}
    data2 = data1.groupby(['type', 'field_value']).agg(num_agg).reset_index()
    data2.columns = ['type', 'field_value', 'systems', 'system_type', 'fields', 'field_name']
    logger.info("data2: unique field_name running ...")
    data2['field_name'] = data2['field_name'].apply(unique_field)
    data2['ctime'] = strftime('%Y-%m-%d %H:%M:%S', localtime())
    data2 = data2[['type', 'field_value', 'systems', 'fields', 'system_type', 'field_name', 'ctime']]

    logger.info("delete old data ")
    common.execute_sql("""delete from ecology.audit_associations_detail where type={}""".format(type))
    common.execute_sql("""delete from ecology.audit_associations where type={}""".format(type))
    common.execute_sql("""delete from ecology.audit_associations_stat where type={}""".format(type))

    logger.info("copying data to database")
    common.copy_to_DB_from_DF(data, 'ecology.audit_associations_detail')
    common.copy_to_DB_from_DF(data1, 'ecology.audit_associations')
    common.copy_to_DB_from_DF(data2, 'ecology.audit_associations_stat')
    return

hrmresource = """select distinct employee_num, name, primary_department, secondary_department, tertiary_sector, post, hierarchy, 
    post1, employee_status, gender, birth, job_status, age, entry_date, confirmation_date, resignation_date, 
    company_years, working_date, card_no, nation, marriage, political_outlook, education, university, 
    studied, registered_residence, home_address, contact_number, mobile_phone, location_archives, attendance_no, 
    contract_no, contract_period, emergency_contact, emergency_contact_no, social_title from ecology.hrmresource """


#@running_duration
def audit_name(type):
    data = pd.DataFrame()
    # 读取OA姓名数据
    sql =  f"""select 'A' as system_type,a.name as field_value,'ecology.hrmresource.name' as field_name from ({hrmresource}) a where a.name>'Z' 
        union select 'A' as system_type,member as field_value,'ecology.hrmsocialinfo.member' as field_name from ecology.hrmsocialinfo where member>'Z'
        union select 'A' as system_type,witness as field_value,'ecology.hrmworkresume.witness' as field_name from ecology.hrmworkresume where witness>'Z'
        union select 'A' as system_type,witness as field_value,'ecology.hrmeducationinfo.witness' as field_name from ecology.hrmeducationinfo where witness>'Z'
        union select 'A' as system_type,member as field_value,'ecology.hrmfamilyinfo.member' as field_name from ecology.hrmfamilyinfo where member>'Z'
        union select 'A' as system_type,a.emergency_contact as field_value,'ecology.hrmresource.emergency_contact' as field_name from ({hrmresource}) a where a.emergency_contact>'Z'     
        """
    data = data.append(common.select_sql(sql))
    # 读取停车场用户数据
    sql = """select 'B' as system_type,name as field_value,'car.kt_o_carcard.name' as field_name from car.kt_o_carcard where name>'Z' """
    data = data.append(common.select_sql(sql))
    # 读取采购供应商联系人数据
    sql =  """select 'C' as system_type,contacts as field_value,'purchase.cg_supplier.contacts' as field_name from purchase.cg_supplier where contacts>'Z' 
        union select 'C' as system_type,legal as field_value,'purchase.cg_supplier.contacts' as field_name from purchase.cg_supplier where legal>'Z'
        """
    data = data.append(common.select_sql(sql))
    # 读取嘉码系统联系人数据
    sql =  """select 'D' as system_type,"ARTIFICIAL_PERSON" as field_value,'purchase.jm_supplier.ARTIFICIAL_PERSON' as field_name from purchase.jm_supplier where "ARTIFICIAL_PERSON">'Z' """
    data = data.append(common.select_sql(sql))
    # 读取金蝶系统联系人数据
    sql =  """select 'E' as system_type,fcontactperson as field_value,'purchase.jd_supplier.fcontactperson' as field_name from purchase.jd_supplier where fcontactperson>'Z'
        union select 'E' as system_type,fartificialperson as field_value,'purchase.jd_supplier.fartificialperson' as field_name from purchase.jd_supplier where fartificialperson>'Z'
        """
    data = data.append(common.select_sql(sql))
    # 读取企微系统姓名数据
    sql = """select 'F' as system_type,name as field_value,'wx_work.ods_department_user.name' as field_name from wx_work.ods_department_user where name>'Z'
        """
    data = data.append(common.select_sql(sql))
    # 读取PRM系统姓名数据
    sql = """select 'G' as system_type,bank_name as field_value,'public.ods_contract.bank_name' as field_name from public.ods_contract where bank_name>'Z'
        """
    data = data.append(common.select_sql(sql))
    # 读取Umall店铺联系人数据
    sql = """select 'H' as system_type,manager_name as field_value,'public.ods_shop.manager_name' as field_name from public.ods_shop where manager_name>'Z'
       union select 'H' as system_type,bank_account_name as field_value,'public.ods_shop.bank_account_name' as field_name from public.ods_shop where bank_account_name>'Z'
        """
    data = data.append(common.select_sql(sql))
    # 读取Umall用户姓名数据
    sql = """select 'I' as system_type,name as field_value,'public.dwd_user_info.name' as field_name from public.dwd_user_info where name>'Z'
       """
    data = data.append(common.select_sql(sql))

    # 简单清洗并入库
    data['field_value'] = data['field_value'].apply(field_format)
    data = data[data['field_value'].str.encode('utf-8').apply(len) > 3]
    insert_audit(data, type)


@running_duration
def audit_phone(type):
    data = pd.DataFrame()
    # 读取OA手机号数据
    sql = f"""select 'A' as system_type,a.contact_number as field_value,'ecology.hrmresource.contact_number' as field_name from ({hrmresource}) a where a.contact_number>'' 
        union select 'A' as system_type,a.mobile_phone as field_value,'ecology.hrmresource.mobile_phone' as field_name from ({hrmresource}) a where a.mobile_phone>''
        union select 'A' as system_type,a.emergency_contact_no as field_value,'ecology.hrmresource.emergency_contact_no' as field_name from ({hrmresource}) a where a.emergency_contact_no>''
        union select 'A' as system_type,witness_no as field_value,'ecology.hrmeducationinfo.witness_no' as field_name from ecology.hrmeducationinfo where witness_no>''
        union select 'A' as system_type,witness_no as field_value,'ecology.hrmworkresume.witness_no' as field_name from ecology.hrmworkresume where witness_no>''
        union select 'A' as system_type,telephone as field_value,'ecology.hrmfamilyinfo.telephone' as field_name from ecology.hrmfamilyinfo where telephone>''
        union select 'A' as system_type,telephone as field_value,'ecology.hrmsocialinfo.telephone' as field_name from ecology.hrmsocialinfo where telephone>''
        """
    data = data.append(common.select_sql(sql))

    # 读取采购供应商联系人手机号数据
    sql = """select 'C' as system_type,contacts_no as field_value,'purchase.cg_supplier.contacts_no' as field_name from purchase.cg_supplier where contacts_no>''
        """
    data = data.append(common.select_sql(sql))

    # 读取嘉码系统联系人手机号数据
    sql = """select 'D' as system_type,"ARTIFICIAL_TELEPHONE" as field_value,'purchase.jm_supplier.ARTIFICIAL_TELEPHONE' as field_name from purchase.jm_supplier where "ARTIFICIAL_TELEPHONE">'' """
    data = data.append(common.select_sql(sql))

    # 读取金蝶系统联系人手机号数据
    sql = """select 'E' as system_type,fphone as field_value,'purchase.jd_supplier.fphone' as field_name from purchase.jd_supplier where fphone>''
       union select 'E' as system_type,fmobile as field_value,'purchase.jd_supplier.fmobile' as field_name from purchase.jd_supplier where fmobile>''
       union select 'E' as system_type,fphone1 as field_value,'purchase.jd_supplier.fphone1' as field_name from purchase.jd_supplier where fphone1>''
       union select 'E' as system_type,fmobile1 as field_value,'purchase.jd_supplier.fmobile1' as field_name from purchase.jd_supplier where fmobile1>''
        """
    data = data.append(common.select_sql(sql))

    # 读取企微系统姓名手机号数据
    sql = """select 'F' as system_type,mobile as field_value,'wx_work.ods_department_user.mobile' as field_name from wx_work.ods_department_user where mobile>'' """
    data = data.append(common.select_sql(sql))

    # 读取PRM系统姓名手机号数据
    sql = """select 'G' as system_type,supplier_phone as field_value,'public.ods_ly_contract.supplier_phone' as field_name from public.ods_ly_contract where supplier_phone>''
           """
    data = data.append(common.select_sql(sql))
    # 读取Umall用户店铺联系人手机号数据
    sql = """select 'H' as system_type,manager_mobile::text as field_value,'public.ods_shop.manager_mobile' as field_name from public.ods_shop where manager_mobile>0
       union select 'H' as system_type,service_phone as field_value,'public.ods_shop.service_phone' as field_name from public.ods_shop where service_phone>''
       union select 'H' as system_type,aid::text as field_value,'public.ods_assistant_shop.aid' as field_name from public.ods_assistant_shop where aid>0
        """
    data = data.append(common.select_sql(sql))
    # 读取Umall用户手机号数据
    sql = """select 'I' as system_type,gid::text as field_value,'public.dwd_user_info.gid' as field_name from public.dwd_user_info where gid>0
          """
    data = data.append(common.select_sql(sql))

    # 简单清洗并入库
    data['field_value'] = data['field_value'].apply(field_format)
    data = data[data['field_value'].str.len() > 7]
    insert_audit(data, type)


#@running_duration
def audit_plate(type):
    data = pd.DataFrame()
    # 读取停车场车牌数据
    sql = """select 'B' as system_type,case when province>'' then province||carplatenum else carplatenum end as field_value,'car.kt_o_carcard.carplatenum' as field_name from car.kt_o_carcard where carplatenum>'' """
    data = data.append(common.select_sql(sql))

    # 读取Umall用户绑定车牌数据
    sql = """select 'I' as system_type,plate_no as field_value,'public.ods_license_plate.plate_no' as field_name from public.ods_license_plate where plate_no>''
          """
    data = data.append(common.select_sql(sql))

    # 简单清洗并入库
    data['field_value'] = data['field_value'].apply(field_format)
    data = data[data['field_value'].str.len() > 1]
    insert_audit(data, type)


#@running_duration
def audit_idcard(type):
    data = pd.DataFrame()
    # 读取OA简历身份证号码数据
    sql = f"""select 'A' as system_type,a.card_no as field_value,'ecology.hrmresource.card_no' as field_name from ({hrmresource}) a where a.card_no>'' 
        """
    data = data.append(common.select_sql(sql))

    # 读取PRM系统结算开户人证件号数据
    sql = """select 'G' as system_type,bank_card as field_value,'public.ods_contract.bank_card' as field_name from public.ods_contract where bank_card>''
        """
    data = data.append(common.select_sql(sql))

    # 读取Umall用户身份证数据
    sql = """select 'I' as system_type,id_card as field_value,'public.dwd_user_info.id_card' as field_name from public.dwd_user_info where id_card>''
          """
    data = data.append(common.select_sql(sql))

    # 简单清洗并入库
    data['field_value'] = data['field_value'].apply(field_format)
    data = data[data['field_value'].str.len() > 1]
    insert_audit(data, type)


#@running_duration
def audit_bankno(type):
    data = pd.DataFrame()
    # 读取采购供应商银行卡号数据
    sql = """select 'C' as system_type,account as field_value,'purchase.cg_supplier.account' as field_name from purchase.cg_supplier where account>'' """
    data = data.append(common.select_sql(sql))

    # 读取金蝶系统开户行账号数据
    sql = """select 'E' as system_type,fbankaccount as field_value,'purchase.jd_supplier.fbankaccount' as field_name from purchase.jd_supplier where fbankaccount>'' """
    data = data.append(common.select_sql(sql))

    # 读取PRM系统结算账号数据
    sql = """select 'G' as system_type,bank_no as field_value,'public.ods_contract.bank_no' as field_name from public.ods_contract where bank_no>'' """
    data = data.append(common.select_sql(sql))

    # 读取Umall店铺银行帐号数据
    sql = """select 'H' as system_type,bank_account_number as field_value,'public.ods_shop.bank_account_number' as field_name from public.ods_shop where bank_account_number>'' """
    data = data.append(common.select_sql(sql))

    # 简单清洗并入库
    data['field_value'] = data['field_value'].apply(field_format)
    data = data[data['field_value'].str.len() > 1]
    insert_audit(data, type)


#@running_duration
def audit_ename(type):
    data = pd.DataFrame()
    # 读取采购系统公司名称
    sql = """select 'C' as system_type,"supplier_name" as field_value,'purchase.cg_supplier.supplier_name' as field_name from purchase.cg_supplier where "supplier_name">'Z' """
    data = data.append(common.select_sql(sql))

    # 读取嘉码系统公司名称
    sql = """select 'D' as system_type,"COMPANY_NAME" as field_value,'purchase.jm_supplier.COMPANY_NAME' as field_name from purchase.jm_supplier where "COMPANY_NAME">'Z' """
    data = data.append(common.select_sql(sql))

    # 读取金蝶系统供应商名称
    sql = """select 'E' as system_type,fname_l2 as field_value,'purchase.jd_supplier.fname_l2' as field_name from purchase.jd_supplier where fname_l2>'Z' """
    data = data.append(common.select_sql(sql))

    # 读取企微系统职务（店铺）
    sql = """select 'F' as system_type,external_position as field_value,'wx_work.ods_deparment_user.external_position' as field_name from wx_work.ods_department_user where external_position>'Z' """
    data = data.append(common.select_sql(sql))

    # 读取PRM系统品牌名称
    sql = """select 'G' as system_type,brand_name as field_value,'public.ods_contract.brand_name' as field_name from public.ods_contract where brand_name>'Z'
       union select 'G' as system_type,brand_name as field_value,'public.ods_ly_contract.brand_name' as field_name from public.ods_ly_contract where brand_name>'Z'
        """
    data = data.append(common.select_sql(sql))
    # 读取Umall用户店铺名称
    sql = """select 'H' as system_type,sname as field_value,'public.ods_shop.sname' as field_name from public.ods_shop where sname>'Z' """
    data = data.append(common.select_sql(sql))

    # 简单清洗并入库
    data['field_value'] = data['field_value'].apply(field_format)
    data = data[data['field_value'].str.len() > 1]
    insert_audit(data, type)


if __name__ == '__main__':

    start_time = datetime.datetime.now()
    work_dir, log_dir, mid, connect_db, postgres_host, postgres_port, postgres_user, postgres_password, postgres_datebase = common.get_sys_config()
    logger = common.bigdata_logger('audit.log')
    logger.info('Audit_stat starting ...')

    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('precision', 3)

    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('-s', dest="stage")
    args = parser.parse_args()

    if args.stage is not None and args.stage.lower() == '0':  # 供调试使用，在下面加入需要调试的模块
        # type = 0
        # common.execute_sql("""delete from ecology.audit_associations_detail where type>{}""".format(type))
        # common.execute_sql("""delete from ecology.audit_associations where type>{}""".format(type))
        # common.execute_sql("""delete from ecology.audit_associations_stat where type>{}""".format(type))
        audit_ename(6)
    else:
        """ type:
        1:  姓名
        2:  手机号
        3:  车牌号
        4:  身份证
        5:  银行卡
        6:  名称  """
        audit_name(1)
        audit_phone(2)
        audit_plate(3)
        audit_idcard(4)
        audit_bankno(5)
        audit_ename(6)
        print('-'*30)

    logger.info("Audit_stat Total duration {}".format(datetime.datetime.now() - start_time))
