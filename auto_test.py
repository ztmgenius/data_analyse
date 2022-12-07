#!/usr/local/bin/python3
# *_*coding:utf-8 *_*
__author__ = "Linlefeng"
"""
    自动化测试
"""
from selenium import webdriver  # 用于打开网站
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait

import re  # 用于正则
from PIL import Image  # 用于打开图片和对图片处理
import pytesseract  # 用于图片转文字

import base64
import json
import requests

import time, os, sys

chromedriver = r"C:\Users\Administrator\Downloads\chromedriver_win32 (1)\chromedriver.exe"
url = 'https://t.shoppingm.cn/'


def baidu():
    # global browser
    option = webdriver.ChromeOptions()
    option.add_experimental_option("detach", True)
    browser = webdriver.Chrome(chromedriver, options=option)
    # browser = webdriver.Chrome()
    # 通过get方法发送网址
    browser.get("https://www.baidu.com")
    # 设置停顿在页面的秒数
    time.sleep(1)
    # 查找id名为kw的页面元素，模拟键盘输入值测试
    browser.find_element_by_id('kw').send_keys("测试")
    # 查找id名为su的页面元素，模拟鼠标进行点击
    browser.find_element_by_id('su').click()
    # 设置停顿在页面的秒数
    time.sleep(1)
    # 查找id名为kw的页面元素，进行清空搜索栏
    browser.find_element_by_id('kw').clear()
    # 设置停顿在页面的秒数
    time.sleep(2)
    # 退出测试并关闭浏览器


def qyweixin():
    option = webdriver.ChromeOptions()
    option.add_experimental_option("detach", True)
    browser = webdriver.Chrome(chromedriver, options=option)
    # 通过get方法发送网址
    browser.get("https://work.weixin.qq.com/wework_admin/loginpage_wx?redirect_uri=https%3A%2F%2Fwork.weixin.qq.com%2Fwework_admin%2Fframe")
    # 设置停顿在页面的秒数
    time.sleep(15)
    # 查找id名为kw的页面元素，模拟键盘输入值测试
    browser.find_element_by_id("menu_customer").click()  # 客户联系
    time.sleep(1)
    browser.find_element_by_xpath("/html/body/div/div/div/main/div/div[1]/div/div[1]/ul[1]/li[2]/a").click()  # 客户群
    time.sleep(2)
    browser.find_element_by_xpath("/html/body/div[1]/div/div/main/div/div[1]/div/div[2]/div/div[2]/div[1]/div/div[2]/input").send_keys('于建飞')  # 搜索


class VerificationCode:
    def __init__(self):
        option = webdriver.ChromeOptions()
        option.add_experimental_option("detach", True)
        self.driver = webdriver.Chrome(chromedriver, options=option)
        self.find_element = self.driver.find_element_by_css_selector
    def get_pictures(self):
        self.driver.get(url)  # 打开登陆页面
        time.sleep(1)
        self.driver.save_screenshot('picture.png')  # 全屏截图
        page_snap_obj = Image.open('picture.png')
        img = self.find_element('#imgObj')  # 验证码元素位置
        time.sleep(1)
        location = img.location
        size = img.size  # 获取验证码的大小参数
        left = location['x']
        top = location['y']
        right = left + size['width']
        bottom = top + size['height']
        image_obj = page_snap_obj.crop((left, top, right, bottom))  # 按照验证码的长宽，切割验证码
        # image_obj.show()  # 打开切割后的完整验证码
        image_obj.save('picture.png')  # 保存切割后的完整验证码
        # self.driver.close()  # 处理完验证码后关闭浏览器
        return image_obj

    def processing_image(self):
        image_obj = self.get_pictures()  # 获取验证码
        img = image_obj.convert("L")  # 转灰度
        pixdata = img.load()
        w, h = img.size
        threshold = 160
        # 遍历所有像素，大于阈值的为黑色
        for y in range(h):
            for x in range(w):
                if pixdata[x, y] < threshold:
                    pixdata[x, y] = 0
                else:
                    pixdata[x, y] = 255
        return img

    def delete_spot(self):
        images = self.processing_image()
        data = images.getdata()
        w, h = images.size
        black_point = 0
        for x in range(1, w - 1):
            for y in range(1, h - 1):
                mid_pixel = data[w * y + x]  # 中央像素点像素值
                if mid_pixel < 50:  # 找出上下左右四个方向像素点像素值
                    top_pixel = data[w * (y - 1) + x]
                    left_pixel = data[w * y + (x - 1)]
                    down_pixel = data[w * (y + 1) + x]
                    right_pixel = data[w * y + (x + 1)]
                    # 判断上下左右的黑色像素点总个数
                    if top_pixel < 10:
                        black_point += 1
                    if left_pixel < 10:
                        black_point += 1
                    if down_pixel < 10:
                        black_point += 1
                    if right_pixel < 10:
                        black_point += 1
                    if black_point < 1:
                        images.putpixel((x, y), 255)
                    black_point = 0
        # images.show()
        return images

    def image_str(self):
        image = self.delete_spot()
        # return
        tessdata_dir = '--tessdata-dir "C:\Program Files (x86)\Tesseract-OCR"'
        pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe"  # 设置pyteseract路径
        result = pytesseract.image_to_string(image, lang="eng", config=tessdata_dir)  # 图片转文字
        resultj = re.sub(u"([^\u4e00-\u9fa5\u0030-\u0039\u0041-\u005a\u0061-\u007a])", "", result)  # 去除识别出来的特殊字符
        result_four = resultj[0:5]  # 只获取前4个字符
        return result_four

    def base64_api(self,uname, pwd, img, typeid):
        with open(img, 'rb') as f:
            base64_data = base64.b64encode(f.read())
            b64 = base64_data.decode()
        data = {"username": uname, "password": pwd, "typeid": typeid, "image": b64}
        result = json.loads(requests.post("http://api.ttshitu.com/predict", json=data).text)
        if result['success']:
            return result["data"]["result"]
        else:
            return result["message"]
        return ""

    def image2str(self):
        self.get_pictures()
        img_path = "picture.png"
        result = self.base64_api(uname='linlefeng401', pwd='llfllf401', img=img_path, typeid=3)
        os.remove(img_path)
        return result


def umall():
    vcode = VerificationCode()
    browser = vcode.driver
    browser.maximize_window()
    browser.implicitly_wait(10)
    time.sleep(1)
    # 设置停顿在页面的秒数
    check_code = vcode.image2str()
    browser.find_element_by_id("checkCode").send_keys(check_code)
    browser.find_element_by_id("j_username").send_keys('linlefeng')
    browser.find_element_by_id("j_password").send_keys('Mall321#@')
    browser.find_element_by_id("j_password").send_keys(Keys.TAB)
    browser.find_element_by_id("j_password").send_keys(Keys.TAB)
    browser.find_element_by_id("submitBtn").click()  # 登陆

    ActionChains(browser).move_to_element(browser.find_element_by_link_text("CRM")).perform()  # CRM
    down_data_click = WebDriverWait(browser, 5).until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div/div/div/div[2]/div/ul/li[7]/ul/li[24]/a")))  # 新版会员分析
    down_data_click.click()

    ActionChains(browser).move_to_element(browser.find_element_by_link_text("数据中心")).click()
    browser.find_element_by_xpath("/html/body/div[1]/div/div[2]/ul/li[6]/a").click()  # 数据中心
    browser.find_element_by_xpath("/html/body/div[2]/div[1]/div/ul/li[3]/dl/dd[8]/a").click()  # 企微月统计

    browser.switch_to.frame("mainIframe")
    browser.find_element_by_id("startDate").click()  # 选择月份
    browser.find_element_by_id("search").click()  # 查询

    # browser.quit()


if __name__ == '__main__':
    umall()
    # input("input:")

