"""
 * @Descripttion: 用于爬取贝壳网小区信息、二手房信息、租房房源信息
 * @version:
 * @Author: linlefeng
 * @Date: 2021-06-16
 * @LastEditors: linlefeng
 * @LastEditTime: 2021-06-17
 """
import requests
import time
import datetime
from lxml import etree
import xlsxwriter
import re
import pandas as pd

import common


def get_html(page):
    """获取网站html代码"""
    url = "https://cc.zu.ke.com/zufang/pg{}/#contentList".format(page)
    headers = {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
    }
    response = requests.get(url, headers=headers).text
    return response


def parse_html(htmlcode, data):
    """解析html代码"""
    content = etree.HTML(htmlcode)
    results = content.xpath('///div[@class="content__article"]/div[1]/div')
    for result in results[:]:
        community = result.xpath('./div/p[@class="content__list--item--title"]/a/text()')[0].replace('\n',
                    '').strip().split()[0] \
                    if len(result.xpath('./div/p[@class="content__list--item--title"]/a/text()')) > 0 else ""
        address = "-".join(result.xpath('./div/p[@class="content__list--item--des"]/a/text()'))
        landlord = result.xpath('./div/p[@class="content__list--item--brand oneline"]/span[@class="brand"]/text()')[0].replace('\n'
            ,'').strip().split()[0] if len(result.xpath('./div/p[@class="content__list--item--brand oneline"]/span[@class="brand"]/text()')) > 0 else ""
        if landlord=='':
            postime = result.xpath('./div/p/span[@class="content__list--item--time oneline"]/text()')[0].replace('\n',
                    '').strip().split()[0] if len(result.xpath('./div/p/span[@class="content__list--item--time oneline"]/text()')) > 0 else ""
        else:
            postime = result.xpath('./div/p/span[@class="content__list--item--time"]/text()')[0].replace('\n','').strip().split()[
                0] if len(result.xpath('./div/p/span[@class="content__list--item--time"]/text()')) > 0 else ""
        introduction = ",".join(result.xpath('./div/p[@class="content__list--item--bottom oneline"]/i/text()'))
        price = result.xpath('./div/span/em/text()')[0]
        description = "".join(result.xpath('./div/p[2]/text()')).replace('\n', '').replace('-', '').strip().split()
        area = description[0]
        count = len(description)
        if count == 6:
            orientation = description[1] + description[2] + description[3] + description[4]
        elif count == 5:
            orientation = description[1] + description[2] + description[3]
        elif count == 4:
            orientation = description[1] + description[2]
        elif count == 3:
            orientation = description[1]
        else:
            orientation = ""
        pattern = description[-1]
        floor = "".join(result.xpath('./div/p[2]/span/text()')[1].replace('\n', '').strip().split()).strip() if len(
            result.xpath('./div/p[2]/span/text()')) > 1 else ""
        date_time = time.strftime("%Y-%m-%d", time.localtime())
        """数据存入字典"""
        data_dict = {
            "community": community,
            "address": address,
            "landlord": landlord,
            "postime": postime,
            "introduction": introduction,
            "price": '￥' + price,
            "area": area,
            "orientation": orientation,
            "pattern": pattern,
            "floor": floor,
            "date_time": date_time
        }

        data.append(data_dict)
        logger.debug(data_dict)


def cc_zufang():  # 租房信息
    all_datas = []
    """网站总共100页，循环100次"""
    for page in range(1, 2):
        html = get_html(page)
        parse_html(html, all_datas)


def parse_ershoufang_html(city, district0, data):
    """解析html代码
    """
    htmlcode = get_ershoufang_html(city, district0, 1)
    content = etree.HTML(htmlcode)
    block_num = content.xpath('//*[@id="beike"]/div[1]/div[4]/div[1]/div[2]/h2/span/text()')[0].strip() if len(
        content.xpath('//*[@id="beike"]/div[1]/div[4]/div[1]/div[2]/h2/span/text()')) > 0 else "0"
    logger.info("{} {} 小区个数： {}".format(city, district0, block_num))
    i = 0
    for page in range(1, 40+1):
        htmlcode = get_ershoufang_html(city, district0, page)
        content = etree.HTML(htmlcode)
        # results = content.xpath('//*[@id="beike"]/div[1]/div[4]/div[1]/div[3]/ul/li')  # 按路径查找
        results = content.xpath('//div[@data-component="list"]/ul/li')  # 按关键字查找，两种写法都可以

        data1 = []
        for result in results[:]:  # 循环当前页的所有小区
            community = result.attrib['data-action']
            resblock_id = community.split("&")[2].split("=")[1]
            resblock_name = community.split("&")[3].split("=")[1]
            date_time = time.strftime("%Y-%m-%d", time.localtime())
            sell_num_90 = result.xpath('./div[1]/div[2]/a[1]/text()')[0].strip() if len(result.xpath('./div[1]/div[2]/a[1]/text()')) > 0 else ""
            sell_num_90 = re.findall(r"\d+\.?\d*", sell_num_90)[1]
            renting_num = result.xpath('./div[1]/div[2]/a[2]/text()')[0].strip() if len(result.xpath('./div[1]/div[2]/a[2]/text()')) > 0 else "0"
            renting_num = re.findall(r"\d+\.?\d*", renting_num)[0]
            district    = result.xpath('./div[1]/div[3]/a[1]/text()')[0].strip() if len(result.xpath('./div[1]/div[3]/a[1]/text()')) > 0 else ""
            bizcircle   = result.xpath('./div[1]/div[3]/a[2]/text()')[0].strip() if len(result.xpath('./div[1]/div[3]/a[2]/text()')) > 0 else ""
            taglist     = result.xpath('./div[1]/div[4]/span/text()')[0] if len(result.xpath('./div[1]/div[4]/span/text()')) > 0 else ""
            price_avg   = result.xpath('./div[2]/div[1]/div[1]/span/text()')[0].strip() if len(result.xpath('./div[2]/div[1]/div[1]/span/text()')) > 0 else ""
            price_avg   = '0' if price_avg =='暂无数据' else price_avg
            selling_num = result.xpath('./div[2]/div[2]/a/span/text()')[0].strip() if len(result.xpath('./div[2]/div[2]/a/span/text()')) > 0 else ""
            """数据存入字典
            """
            data_dict = {
                "city": city,
                "district": district,
                "resblock_id": resblock_id,
                "resblock_name": resblock_name,
                "sell_num_90": int(sell_num_90),
                "renting_num": int(renting_num),
                "bizcircle": bizcircle,
                "taglist": taglist,
                "price_avg": int(price_avg),
                "selling_num": int(selling_num),
                "date_time": date_time
            }
            data1.append(data_dict)

        # data1 = [dict(t) for t in set([tuple(d.items()) for d in data1])]
        # data1 = sorted(data1, key=lambda x: x['district']+x['resblock_id'])
        data += data1
        i += 30
        if i >= int(block_num):
            break


def save_excel_ershoufang(city, data):  # 按城市保存为excel文件
    """将字典数据写入excel"""
    workbook = xlsxwriter.Workbook('''./beike_ershoufang.{}.xlsx'''.format(city))
    worksheet = workbook.add_worksheet('二手房信息')
    columns = [['城市', 'city'],
               ['行政区域', 'district'],
               ['小区ID', 'resblock_id'],
               ['小区名称', 'resblock_name'],
               ['正在出租数量', 'renting_num'],
               ['在售数量', 'selling_num'],
               ['90天内销售数量', 'sell_num_90'],
               ['销售均价', 'price_avg'],
               ['商圈', 'bizcircle'],
               ['卖点', 'taglist'],
               ['日期', 'date_time']
              ]
    for i, col in enumerate(columns):
        """设置标题加粗"""
        bold_format = workbook.add_format({'bold': True})
        worksheet.write(chr(ord('A')+i)+'1', col[0], bold_format)
    row = 1
    for d in data:
        for col, name in enumerate(columns):
            if type(d[name[1]]) == int:
                worksheet.write_number(row, col, d[name[1]])
            else:
                worksheet.write_string(row, col, d[name[1]])
        row += 1
    workbook.close()


def get_ershoufang_html(city, district, page):
    """获取网站html代码"""
    url = "https://{0}.ke.com/xiaoqu/{1}/pg{2}/".format(city, district, page)
    headers = {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
    }
    response = requests.get(url, headers=headers).text
    return response


def cc_ershoufang():  # 查询在售二手房
    cityp_list = [
        {'city': 'cc', 'district': ['erdaoqu', 'nonganxian', 'chaoyangqu', 'nanguanqu', 'shuangyangqu', 'kuanchengqu',
                                 'dehuishi', 'jingyuequ', 'yushushi', 'qichechanyekaifaqu', 'jingkaibeiqu', 'jingkaiqu1',
                                 'lvyuanqu', 'gaoxinbeiqu', 'gaoxinqu16', 'jiutaiqu']},
        {'city': 'jl', 'district': ['fengmanqu', 'chuanyingqu', 'changyiqu', 'longtanqu',
                                 'huadianshi', 'yongjixian', 'panshishi', 'shulanshi', 'jiaoheshi']}
    ]

    for city in cityp_list:
        all_datas = []
        for district in city['district']:
            parse_ershoufang_html(city['city'], district, all_datas)

        # save_excel_ershoufang(city['city'], all_datas)
        all_datas = pd.DataFrame(all_datas)
        logger.info('STAT {0} 贝壳小区总数： {1}'.format(city['city'], len(all_datas)))
        common.execute_sql("""delete from wx_work.beike_ershoufang where to_char(date_time,'yyyy-mm-dd')='{0}' and city='{1}'"""
                    .format(time.strftime("%Y-%m-%d", time.localtime()), city['city']))
        if common.insert_to_DB_from_DF(all_datas, 'wx_work.beike_ershoufang', 1000) == 1:
            logger.error('贝壳二手房入库出错 city:{}'.format(city['city']))


def get_loupan_html(city, page):
    """获取网站html代码"""
    url = "https://{0}.fang.ke.com/loupan/pg{1}/".format(city, page)
    headers = {
        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
    }
    response = requests.get(url, headers=headers).text
    import random
    time.sleep(random.randint(5, 10))
    return response


def parse_loupan_html(city, data):
    """解析html代码
    """
    htmlcode = get_loupan_html(city, 1)
    content = etree.HTML(htmlcode)
    block_num = content.xpath('/html/body/div[6]/div[2]/span[2]/text()')[0].strip() if len(
        content.xpath('/html/body/div[6]/div[2]/span[2]/text()')) > 0 else "0"
    block_num = int(block_num)
    # print('STAT:', city, "在售楼盘个数：", block_num)
    i = 0
    for page in range(1, 100+1):
        htmlcode = get_loupan_html(city, page)
        content = etree.HTML(htmlcode)
        results = content.xpath('/html/body/div[6]/ul[2]/li')  # 按路径查找
        data1 = []
        for result in results[:]:  # 循环当前页的所有小区
            resblock_name = result.xpath('./div/div[1]/a/text()')
            resblock_name = resblock_name[0].strip() if len(resblock_name) > 0 else ""
            resblock_type = result.xpath('./div/div[1]/span[2]/text()')
            resblock_type = resblock_type[0].strip() if len(resblock_type) > 0 else ""
            status        = result.xpath('./div/div[1]/span[1]/text()')
            status        = status[0].strip() if len(status) > 0 else ""
            location      = result.xpath('./div/a[1]/text()')
            location      = location[1].replace('\n', '').replace('\t', '').strip() if len(location) > 0 else ""
            rooms         = result.xpath('./div/a[2]/span')
            price_avg     = result.xpath('./div/div[4]/div[1]/span[1]/text()')
            price_avg     = price_avg[0].strip() if len(price_avg) > 0 else ""
            price_total   = result.xpath('./div/div[4]/div[2]/text()')
            price_total   = price_total[0].strip() if len(price_total) > 0 else ""
            price_total   = price_total[2:]
            district      = location.split("/")[0]
            bizcircle     = location.split("/")[1]
            location      = location.split("/")[2]
            room_type = ''
            for room in rooms[:]:
                if "户型" in room.text:
                    continue
                if "建面" in room.text:
                    area = room.text[2:]
                else:
                    room_type += room.text + " "
            room_type = room_type.strip()
            date_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            """数据存入字典"""
            data_dict = {
                "city": city,
                "district": district,
                "bizcircle": bizcircle,
                "resblock_name": resblock_name,
                "resblock_type": resblock_type,
                "status": status,
                "location": location,
                "room_type": room_type,
                "area": area,
                "price_avg": price_avg,
                "price_total": price_total,
                "date_time": date_time
            }
            data1.append(data_dict)
        data += data1
        i += 10
        if i >= block_num:
            break


def cc_loupan():  # 查询在售新楼盘
    cityp_list = ['cc', 'jl']
    for city in cityp_list:
        all_datas = []
        parse_loupan_html(city, all_datas)
        # all_datas = [dict(t) for t in set([tuple(d.items()) for d in all_datas])]
        all_datas = pd.DataFrame(all_datas)
        if len(all_datas) == 0:
            logger.warning('STAT city：{0} 贝壳在售楼盘总数：{1} 被反爬虫了'.format(city, len(all_datas)))
            continue
        logger.info('STAT {0} 贝壳在售楼盘总数：{1}'.format(city, len(all_datas)))
        common.execute_sql("""delete from wx_work.beike_loupan where to_char(date_time,'yyyy-mm-dd')='{0}' and city='{1}'"""
                    .format(time.strftime("%Y-%m-%d", time.localtime()), city))
        if common.insert_to_DB_from_DF(all_datas, 'wx_work.beike_loupan', 1000) == 1:
            logger.error('贝壳新楼盘入库出错 city:{}'.format(city))
        time.sleep(10)


if __name__ == '__main__':
    work_dir, connect_db, postgres_host, postgres_port, postgres_user, postgres_password, postgres_datebase = common.get_config()
    logger = common.bigdata_logger('beike.log')
    logger.info('beike starting ...')

    start_time = datetime.datetime.now()
    pd.set_option('display.max_columns', 1000000)
    pd.set_option('display.width', 1000000)
    pd.set_option('precision', 3)
    # cc_zufang()
    cc_ershoufang()
    # cc_loupan()
    logger.info("Total duration of spider:{}".format(datetime.datetime.now() - start_time))