# coding:utf-8
# coding: utf-8
import os
import sys
from pymongo import UpdateOne
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from NNW.import_main import ImportMain
from common import handle_mongodb
from common.readconfig import ReadConfig
import traceback
import requests
import random
from bson import json_util
from bson.objectid import ObjectId
import time
import datetime
import json
from xlrd import xldate_as_tuple, xldate_as_datetime
from pytz import timezone
from xpinyin import Pinyin
import requests
import bs4
import time

cst_tz = timezone('Asia/Shanghai')


class ImportSuperiorId(ImportMain):
    def __init__(self):
        super().__init__()
        self.PHPSESSID = self.get_PHPSESSID()
        # self.PHPSESSID = '6980cdb9c06d11278e75a7efff65c55a47a0'

    def main(self):
        self.control()

        # while True:
        #     print(datetime.datetime.now())
        #     self.control()
        #     print(datetime.datetime.now())
        #     print('运行结束')
        #     time.sleep(300)

    def control(self, type=1):
        print('start ImportSuperiorId control')

        self.ini_query_time(type)

        self.download_html()
        self.read_html()

    def ini_query_time(self, type=1):
        now_time = datetime.datetime.now()

        if type == 2:
            self.datetime_before_1_day = now_time + datetime.timedelta(days=-1)
            self.year_before = self.datetime_before_1_day.year
            self.month_before = self.datetime_before_1_day.month
            self.day_before = self.datetime_before_1_day.day
            self.hour_before = self.datetime_before_1_day.hour
            self.minute_before = self.datetime_before_1_day.minute
            self.second_before = self.datetime_before_1_day.second
        else:
            self.datetime_before = now_time + datetime.timedelta(minutes=-40)
            self.year_before = self.datetime_before.year
            self.month_before = self.datetime_before.month
            self.day_before = self.datetime_before.day
            self.hour_before = self.datetime_before.hour
            self.minute_before = self.datetime_before.minute
            self.second_before = self.datetime_before.second

    def read_html(self):
        path = self.file_path + "ts_superior.html"
        # path = "D:\\python_work_erp\\NNW\\ts_superior.html"
        # path = "/project/erp/python_work/NNW/ts_superior.html"

        with open(path, 'r', encoding="utf-8") as f:
            Soup = bs4.BeautifulSoup(f.read(), 'html.parser')

            get_divs = Soup.select('div')
            for d in get_divs:
                if '用户登录过期' in d:
                    print('用户登录过期')
                    return False

            get_columns = Soup.select('table tr th')
            get_values = Soup.select('table tr td')

        column = []
        value = []
        for c in get_columns:
            column.append(str(c.string))

        for v in get_values:
            value.append(str(v.string))

        self.import_data(column, value)

    def import_data(self, column, value):
        print('start import_data')

        json = []
        while len(value) > 0:
            temp_data = {}
            for i in range(len(column)):
                temp_index = column[i]

                temp_v = value.pop(0)
                if temp_v == 'None':
                    temp_v = ''

                if str(temp_index) == 'ID':
                    temp_data['ID'] = temp_v
                if str(temp_index) == '上级ID':
                    temp_data['上级ID'] = temp_v

            json.append(temp_data)

        bulk_op = []
        for one in json:
            temp_one = UpdateOne(
                {
                    'DelStatus': 0,
                    'ID': str(one['ID']),
                },
                {
                    '$set': {
                        'shang4_ji2_ID': str(one['上级ID']),
                        'UpdateTime': cst_tz.localize(datetime.datetime.now()),
                    }
                },
                True
            )
            bulk_op.append(temp_one)

        if len(bulk_op) > 0:
            self.col_NNWCustomer.bulk_write(bulk_op)

    def download_html(self):
        url = 'http://fenxiao.feiyang.cn/Admin/UserWeb/listForTuser.html?status=1&level=&group=&is_vip=2&vip_status=&sort_type=&btuser_id=&end_time=&user_name=&user_id=&account=&pwuser_id=&search=&g_down=%E4%B8%8B%E8%BD%BD'
        begin_time = '&begin_time=' + str(self.year_before) + '%2F' + str(str(self.month_before).zfill(2)) + '%2F' + str(str(self.day_before).zfill(
            2)) + '+' + str(str(self.hour_before).zfill(2)) + '%3A' + str(str(self.minute_before).zfill(2)) + '%3A' + str(
            str(self.second).zfill(2))

        url = url + begin_time

        cookie = 'g_theme=; __root_domain_v=.feiyang.cn; Orders_list=remark%2Cnumber%2Cback_cash_one%2Cback_cash_total%2Coperator%2Cbooking_time%2Cpost_info%2Cinvoice_info%2Cic_info; PHPSESSID=' + str(self.PHPSESSID) + '; login_num=0; lost_notice_time=1581955200000; last_bulletin_id=124370; _qddaz=QD.wlzzuy.c8wl04.k6tehkcq; _qdda=3-1.2jffc1; _qddab=3-xw17os.k6x3one6'
        herder = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8",
            "Accept-Encoding": "gzip, deflate, sdch",
            "Cookie": cookie,
            "Connection": "keep-alive"
        }

        r = requests.get(url, headers=herder)

        # open打开excel文件，报存为后缀为xls的文件
        fp = open("ts_superior.html", "wb")
        fp.write(r.content)
        fp.close()

    def get_PHPSESSID(self):
        condition = {
            'String': 'TSPHPSESSID',
        }

        data = self.col_CustomSettings.find_one(condition)

        return data['PHPSESSID']


if __name__ == '__main__':
    start = time.perf_counter()
    ImportSuperiorId = ImportSuperiorId()

    try:
        ImportSuperiorId.main()
    except BaseException as err:
        traceback.print_exc()
        print('__main__ exception: ')
        print(err)
    finally:
        elapsed = (time.perf_counter() - start)
        print("Python Complete, Time used:", elapsed)
