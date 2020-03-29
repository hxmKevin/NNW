# coding:utf-8
# coding: utf-8
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from NNW.import_main import ImportMain
from NNW.import_superior_id import ImportSuperiorId
from NNW.import_order_customer_id import ImportOrderCustomerId
from pymongo import UpdateOne
from common import handle_mongodb
from common.readconfig import ReadConfig
import traceback
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
from common import send_email
cst_tz = timezone('Asia/Shanghai')


class OrderImport(ImportMain):
    def __init__(self):
        super().__init__()
        self.PHPSESSID = self.get_PHPSESSID()#获取seesionId
        # self.PHPSESSID = 'aa1f12dfd1e3a56ff2b2ad821902ab7147a1'
        self.run_i = 0

        self.order_object = {}

    def main(self):
        pass
        # self.download_html_refunded()
        # self.read_html()
        # self.control()
        # self.import_order_data()

        while True:
            self.run_i += 1
            print(datetime.datetime.now())
            print('run_i: ' + str(self.run_i))

            if self.run_i % 600 == 0 or self.run_i == 1:
                self.login()

            if self.run_i % 60 == 0 or self.run_i == 1:
                self.import_expressInfo()

            type = 1
            if self.run_i % 60 == 0 or self.run_i == 1:
                type = 2

            self.ini_query_time(type)

            self.control()

            print(datetime.datetime.now())
            print('运行结束')
            print('')

            time.sleep(4)

    def import_expressInfo(self):
        pass

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

    def import_order_data(self):
        self.download_excel()
        self.read_html()

        if self.run_i % 6 == 0:
            self.download_html_refunded()
            self.read_html('ts_order_refunded')

    def control(self):
        print('start OrderImport control')
        res = self.query_order()
        for one in res:
            self.order_object[str(one['ID'])] = one

        self.import_order_data()

        type = 1
        if self.run_i % 60 == 0:
            type = 2
        ISI = ImportSuperiorId()
        ISI.control(type)

        ioci = ImportOrderCustomerId()
        ioci.update_belong()

    def login(self):
        print('start login')
        url = 'http://fenxiao.feiyang.cn/Admin/Index/login.json'
        data = {'account': self.zj_user_name, 'password': self.zj_user_pwd}

        session = requests.session()

        req_header = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
        }

        # 使用session发起请求
        response = session.post(url, headers=req_header, data=data)
        print(response.text)

        if response.status_code == 200:

            url = 'http://fenxiao.feiyang.cn/Admin/Index/?login=success'

            response = session.get(url, headers=req_header)

            if response.status_code == 200:
                d = session.cookies.get_dict()
                php_session_id = d['PHPSESSID']
                self.update_php_session_id(php_session_id)
                self.PHPSESSID = php_session_id

    def update_php_session_id(self, php_session_id):
        self.col_CustomSettings.update_one(
            {
                "String": "TSPHPSESSID",
            },
            {
                "$set": {"PHPSESSID": str(php_session_id)}
            }
        )

    def query_order(self):
        temp_condition = {
            'DelStatus': 0,
        }

        temp_project = {
            'ID': 1,
            'BelongName': 1,
            'BusinessTurnover': 1,
        }

        res = self.col_NNWOrder.find(temp_condition, temp_project)

        return res

    def read_html(self, file_name='ts_order'):
        path = self.file_path + file_name + ".html"
        # path = "D:\\python_work_erp\\NNW\\" + file_name + ".html"
        # path = "/project/erp/python_work/NNW/ts_order.html"

        with open(path, 'r', encoding="utf-8") as f:
            Soup = bs4.BeautifulSoup(f.read(), 'html.parser')

            get_divs = Soup.select('div')
            for d in get_divs:
                if '用户登录过期' in d:
                    print('用户登录过期')
                    self.run_i = 0
                    return False

            get_columns = Soup.select('table tr th')
            get_values = Soup.select('tr td')

        column = []
        value = []
        for c in get_columns:
            column.append(str(c.string))

        for v in get_values:
            value.append(str(v.string))

        self.import_data(column, value)

    def import_data(self, column, value):
        print('start import_data')
        bulk_op = []

        pinyin_column = []

        for one in column:
            p = Pinyin()
            # 进行拼音转换
            one = p.get_pinyin(one, tone_marks='numbers')

            one = one.replace("-", "_")

            pinyin_column.append(one)

        if len(value) == 0:
            print('没有订单数据')

        while len(value) > 0:
            temp_data = {}
            need_field_num = 0
            for i in range(len(pinyin_column)):
                temp_index = pinyin_column[i]
                temp_v = value.pop(0)

                # 不接受设定范围内的字段
                if str(temp_index) not in self.field_list:
                    continue

                need_field_num += 1
                if temp_v == 'None':
                    temp_v = ''

                if str(temp_index) in self.field_list_float:
                    temp_v = float(temp_v)

                if str(temp_index) in self.field_list_int:
                    temp_v = int(temp_v)

                temp_data[temp_index] = temp_v

            if temp_data['zhi1_fu4_zhuang4_tai4'] == '已支付':
                temp_data['PayStatus'] = 2
            else:
                temp_data['PayStatus'] = 1

            temp_data['OrderTimeISODate'] = self.transfer_time(temp_data['xia4_dan1_shi2_jian1'], 2)
            temp_data['OrderUpdateTimeISODate'] = self.transfer_time(temp_data['geng4_xin1_shi2_jian1'], 2)
            temp_data['DelStatus'] = 0
            temp_data['SalesVolume'] = 0

            try:
                temp_data['SalesVolume'] = int(temp_data['zong3_shu4']) - int(temp_data['yi3_tui4_dan1_shu4'])
            except:
                pass

            temp_data['BusinessTurnover'] = float(temp_data['SalesVolume'] * temp_data['ling2_shou4_jia4_ge2'])
            temp_data['BusinessTurnoverCost'] = float(temp_data['SalesVolume'] * temp_data['cai3_gou4_jia4'])
            temp_data['GrossProfit'] = temp_data['BusinessTurnover'] - temp_data['BusinessTurnoverCost']

            if str(temp_data['ID']) not in self.order_object:
                temp_data['CreateTime'] = cst_tz.localize(datetime.datetime.now())
                temp_data['BelongName'] = ''
                temp_data['BelongId'] = ''
                temp_data['BelongGroup'] = ''
            else:
                temp_data['UpdateTime'] = cst_tz.localize(datetime.datetime.now())

            if 'hui4_yuan2_xin4_xi1' in temp_data:
                if temp_data['hui4_yuan2_xin4_xi1']:
                    temp_res = self.fetch_customer_id(temp_data['hui4_yuan2_xin4_xi1'])
                    temp_data['OrderCustomerId'] = str(temp_res['customer_id'])
                    temp_data['OrderCustomerName'] = str(temp_res['customer_name'])

            temp_one = UpdateOne(
                {
                    'DelStatus': 0,
                    'ID': temp_data['ID'],
                },
                {
                    '$set': temp_data
                },
                True,
            )
            bulk_op.append(temp_one)
            if len(bulk_op) > 1000:
                self.col_NNWOrder.bulk_write(bulk_op)
                bulk_op = []

        if len(bulk_op) > 0:
            self.col_NNWOrder.bulk_write(bulk_op)

    def fetch_customer_id(self, customer_info):
        """
        提取会员id 会员名
        :param customer_info:
        :return:
        """
        content = ''
        try:
            customer_info_split = customer_info.split('ID：')

            customer_id = customer_info_split[1][:-1]
            customer_name = customer_info_split[0][:-1]
        except:
            content = '分解会员id时出错'

        try:
            if int(customer_id) > 0:
                pass
        except:
            content = '会员id格式出错'

        if content:
            self.send_email(content)
            exit()

        return {
            'customer_id': customer_id,
            'customer_name': customer_name,
        }

    def send_email(self, content):
        text = {
            'title': '天时数据导入:' + (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'),
            'content': content,
        }
        send_email.EmailSmtp().send('854641898@qq.com', text)

    def transfer_time(self, date_param, type=1):
        if not date_param:
            res = ''
        elif int(type) == 1:
            res = str(xldate_as_datetime(date_param, 0))
        else:
            cst_tz = timezone('Asia/Shanghai')
            res = cst_tz.localize(datetime.datetime.strptime(date_param, "%Y/%m/%d %H:%M:%S"))
        return res

    def download_html_refunded(self):
        print('start download_html_refunded')
        url = 'http://fenxiao.feiyang.cn/Admin/Orders/list.html?g_dselect%5B%5D=orders_id&g_dselect%5B%5D=code&g_dselect%5B%5D=qrcode&g_dselect%5B%5D=remark_name&g_dselect%5B%5D=remark&g_dselect%5B%5D=user_remark_name&g_dselect%5B%5D=user_remark&g_dselect%5B%5D=supplier_remark_name&g_dselect%5B%5D=supplier_remark&g_dselect%5B%5D=goods_id&g_dselect%5B%5D=alias_name&g_dselect%5B%5D=money_send&g_dselect%5B%5D=out_money_send&g_dselect%5B%5D=money_one&g_dselect%5B%5D=money_total&g_dselect%5B%5D=back_cash_one&g_dselect%5B%5D=back_cash_total&g_dselect%5B%5D=purchase_price&g_dselect%5B%5D=user_money_one&g_dselect%5B%5D=user_money_total&g_dselect%5B%5D=profit&g_dselect%5B%5D=profit_total&g_dselect%5B%5D=user_profit&g_dselect%5B%5D=user_profit_total&g_dselect%5B%5D=amount_total&g_dselect%5B%5D=amount_valid&g_dselect%5B%5D=amount_used&g_dselect%5B%5D=amount_apply&g_dselect%5B%5D=amount_refund&g_dselect%5B%5D=is_pay_name&g_dselect%5B%5D=send_type_name&g_dselect%5B%5D=is_send_name&g_dselect%5B%5D=user_id&g_dselect%5B%5D=user_name&g_dselect%5B%5D=group_id&g_dselect%5B%5D=group_name&g_dselect%5B%5D=tgroup_id&g_dselect%5B%5D=tgroup_name&g_dselect%5B%5D=puser_id&g_dselect%5B%5D=puser_name&g_dselect%5B%5D=supplier_id&g_dselect%5B%5D=supplier_name&g_dselect%5B%5D=scenic_name&g_dselect%5B%5D=player_name&g_dselect%5B%5D=player_mobile&g_dselect%5B%5D=id_number&g_dselect%5B%5D=play_date&g_dselect%5B%5D=start_date&g_dselect%5B%5D=expire_date&g_dselect%5B%5D=start_time&g_dselect%5B%5D=stop_time&g_dselect%5B%5D=player_names&g_dselect%5B%5D=player_mobiles&g_dselect%5B%5D=id_numbers&g_dselect%5B%5D=system_name&g_dselect%5B%5D=other_id&g_dselect%5B%5D=valid_week&g_dselect%5B%5D=out_system_name&g_dselect%5B%5D=out_system_name&g_dselect%5B%5D=out_ext_status_name&g_dselect%5B%5D=seller_code&g_dselect%5B%5D=status_name&g_dselect%5B%5D=seat_names&g_dselect%5B%5D=goods_name&g_dselect%5B%5D=sign_time&g_dselect%5B%5D=create_time&g_dselect%5B%5D=update_time&g_dselect%5B%5D=pay_mode_name&g_dselect%5B%5D=post_address&g_dselect%5B%5D=operator_id&g_dselect%5B%5D=operator_name&g_dselect%5B%5D=operator_group_name&g_dselect%5B%5D=plate_number&g_dselect%5B%5D=rgoods_id&g_dselect%5B%5D=g_system_remark&g_dselect%5B%5D=commission&g_dselect%5B%5D=r_commission&g_dselect%5B%5D=cate_name&g_dselect%5B%5D=s_wuser_id&g_dselect%5B%5D=s_wuser_account&g_dselect%5B%5D=supplier_group_name&g_dselect%5B%5D=time_slot&g_dselect%5B%5D=shuttle_address&g_dselect%5B%5D=purchase_total&g_dselect%5B%5D=cost_money&g_dselect%5B%5D=cost_spread&g_dselect%5B%5D=invoice_info&g_dselect%5B%5D=booking_info&g_dselect%5B%5D=post_tracking_no&status=1&is_used=3&remark_id=&g_stype=1&search_type=g_auto&user_id_type=1&d_expire_time=0天00%3A00%3A00&orders_type=0&g_dfields=%5B"orders_id"%2C"code"%2C"qrcode"%2C"remark_name"%2C"remark"%2C"user_remark_name"%2C"user_remark"%2C"supplier_remark_name"%2C"supplier_remark"%2C"goods_id"%2C"alias_name"%2C"money_send"%2C"out_money_send"%2C"money_one"%2C"money_total"%2C"back_cash_one"%2C"back_cash_total"%2C"purchase_price"%2C"user_money_one"%2C"user_money_total"%2C"profit"%2C"profit_total"%2C"user_profit"%2C"user_profit_total"%2C"amount_total"%2C"amount_valid"%2C"amount_used"%2C"amount_apply"%2C"amount_refund"%2C"is_pay_name"%2C"send_type_name"%2C"is_send_name"%2C"user_id"%2C"user_name"%2C"group_id"%2C"group_name"%2C"tgroup_id"%2C"tgroup_name"%2C"puser_id"%2C"puser_name"%2C"supplier_id"%2C"supplier_name"%2C"scenic_name"%2C"player_name"%2C"player_mobile"%2C"id_number"%2C"play_date"%2C"start_date"%2C"expire_date"%2C"start_time"%2C"stop_time"%2C"player_names"%2C"player_mobiles"%2C"id_numbers"%2C"system_name"%2C"other_id"%2C"valid_week"%2C"out_system_name"%2C"out_system_name"%2C"out_ext_status_name"%2C"seller_code"%2C"status_name"%2C"seat_names"%2C"goods_name%&g_down=%E4%B8%8B%E8%BD%BD&group_id=91103'
        # begin_time = '&begin_time=' + str(self.year_before) + '%2F' + str(
        #     str(self.month_before).zfill(2)) + '%2F' + str(str(self.day_before).zfill(
        #     2)) + '+' + str(str(self.hour_before).zfill(2)) + '%3A' + str(
        #     str(self.minute_before).zfill(2)) + '%3A' + str(
        #     str(self.second_before).zfill(2))

        begin_time = '&begin_time=2020%2F02%2F17+00%3A00%3A00'
        # end_time = begin_time + '&end_time=' + '2020%2F02%2F17+23%3A59%3A59'
        # begin_time = '&begin_time=2020%2F02%2F20+17%3A00%3A00'

        end_time = begin_time + '&end_time='
        # url = url + '&begin_time=2020%2F02%2F19+22%3A00%3A00&end_time=2020%2F02%2F19+23%3A59%3A59'
        url = url + end_time

        cookie = "g_theme=; Orders_list=remark%2Cnumber%2Cback_cash_one%2Cback_cash_total%2Cexport_code%2Cuser_remark_name%2Coperator%2Csupplier_name%2Cbooking_time%2Cpay_mode_name%2Cinvoice_info%2Cic_info%2Cpost_info; PHPSESSID=" + str(
            self.PHPSESSID) + "; login_num=0; __root_domain_v=.feiyang.cn; _qddaz=QD.wlzzuy.c8wl04.k6tehkcq; lost_notice_time=1581955200000; last_bulletin_id=124370; _qdda=3-1.3sjjf; _qddab=3-keyuyx.k6tehkec"

        herder = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8",
            "Accept-Encoding": "gzip, deflate, sdch",
            "Cookie": cookie,
            "Connection": "keep-alive"
        }

        r = requests.get(url, headers=herder)

        # open打开excel文件，报存为后缀为xls的文件
        fp = open("ts_order_refunded.html", "wb")
        fp.write(r.content)
        fp.close()

    def download_excel(self):
        print('start download_excel')
        # url = 'http://fenxiao.feiyang.cn/Admin/Orders/list.html?g_dselect%5B%5D=orders_id&g_dselect%5B%5D=code&g_dselect%5B%5D=qrcode&g_dselect%5B%5D=remark_name&g_dselect%5B%5D=remark&g_dselect%5B%5D=user_remark_name&g_dselect%5B%5D=user_remark&g_dselect%5B%5D=supplier_remark_name&g_dselect%5B%5D=supplier_remark&g_dselect%5B%5D=goods_id&g_dselect%5B%5D=alias_name&g_dselect%5B%5D=money_send&g_dselect%5B%5D=out_money_send&g_dselect%5B%5D=money_one&g_dselect%5B%5D=money_total&g_dselect%5B%5D=back_cash_one&g_dselect%5B%5D=back_cash_total&g_dselect%5B%5D=purchase_price&g_dselect%5B%5D=user_money_one&g_dselect%5B%5D=user_money_total&g_dselect%5B%5D=profit&g_dselect%5B%5D=profit_total&g_dselect%5B%5D=user_profit&g_dselect%5B%5D=user_profit_total&g_dselect%5B%5D=amount_total&g_dselect%5B%5D=amount_valid&g_dselect%5B%5D=amount_used&g_dselect%5B%5D=amount_apply&g_dselect%5B%5D=amount_refund&g_dselect%5B%5D=is_pay_name&g_dselect%5B%5D=send_type_name&g_dselect%5B%5D=is_send_name&g_dselect%5B%5D=user_id&g_dselect%5B%5D=user_name&g_dselect%5B%5D=group_id&g_dselect%5B%5D=group_name&g_dselect%5B%5D=tgroup_id&g_dselect%5B%5D=tgroup_name&g_dselect%5B%5D=puser_id&g_dselect%5B%5D=puser_name&g_dselect%5B%5D=supplier_id&g_dselect%5B%5D=supplier_name&g_dselect%5B%5D=scenic_name&g_dselect%5B%5D=player_name&g_dselect%5B%5D=player_mobile&g_dselect%5B%5D=id_number&g_dselect%5B%5D=play_date&g_dselect%5B%5D=start_date&g_dselect%5B%5D=expire_date&g_dselect%5B%5D=start_time&g_dselect%5B%5D=stop_time&g_dselect%5B%5D=player_names&g_dselect%5B%5D=player_mobiles&g_dselect%5B%5D=id_numbers&g_dselect%5B%5D=system_name&g_dselect%5B%5D=other_id&g_dselect%5B%5D=valid_week&g_dselect%5B%5D=out_system_name&g_dselect%5B%5D=out_other_id&g_dselect%5B%5D=seller_code&g_dselect%5B%5D=status_name&g_dselect%5B%5D=seat_names&g_dselect%5B%5D=goods_name&g_dselect%5B%5D=sign_time&g_dselect%5B%5D=create_time&g_dselect%5B%5D=update_time&g_dselect%5B%5D=pay_mode_name&g_dselect%5B%5D=post_address&g_dselect%5B%5D=operator_id&g_dselect%5B%5D=operator_name&g_dselect%5B%5D=operator_group_name&g_dselect%5B%5D=plate_number&g_dselect%5B%5D=rgoods_id&g_dselect%5B%5D=g_system_remark&g_dselect%5B%5D=commission&g_dselect%5B%5D=r_commission&g_dselect%5B%5D=cate_name&g_dselect%5B%5D=s_wuser_id&g_dselect%5B%5D=s_wuser_account&g_dselect%5B%5D=supplier_group_name&g_dselect%5B%5D=time_slot&g_dselect%5B%5D=shuttle_address&g_dselect%5B%5D=purchase_total&g_dselect%5B%5D=cost_money&g_dselect%5B%5D=cost_spread&g_dselect%5B%5D=invoice_info&g_dselect%5B%5D=booking_info&g_dselect%5B%5D=post_tracking_no&status=1&remark_id=&g_stype=1&search_type=g_auto&user_id_type=1&group_id=91103&d_expire_time=0%E5%A4%A900%3A00%3A00&begin_time=2020%2F02%2F17+00%3A00%3A00&end_time=2020%2F02%2F19+23%3A59%3A59&orders_type=0&g_dfields=%5B%22orders_id%22%2C%22code%22%2C%22qrcode%22%2C%22remark_name%22%2C%22remark%22%2C%22user_remark_name%22%2C%22user_remark%22%2C%22supplier_remark_name%22%2C%22supplier_remark%22%2C%22goods_id%22%2C%22alias_name%22%2C%22money_send%22%2C%22out_money_send%22%2C%22money_one%22%2C%22money_total%22%2C%22back_cash_one%22%2C%22back_cash_total%22%2C%22purchase_price%22%2C%22user_money_one%22%2C%22user_money_total%22%2C%22profit%22%2C%22profit_total%22%2C%22user_profit%22%2C%22user_profit_total%22%2C%22amount_total%22%2C%22amount_valid%22%2C%22amount_used%22%2C%22amount_apply%22%2C%22amount_refund%22%2C%22is_pay_name%22%2C%22send_type_name%22%2C%22is_send_name%22%2C%22user_id%22%2C%22user_name%22%2C%22group_id%22%2C%22group_name%22%2C%22tgroup_id%22%2C%22tgroup_name%22%2C%22puser_id%22%2C%22puser_name%22%2C%22supplier_id%22%2C%22supplier_name%22%2C%22scenic_name%22%2C%22player_name%22%2C%22player_mobile%22%2C%22id_number%22%2C%22play_date%22%2C%22start_date%22%2C%22expire_date%22%2C%22start_time%22%2C%22stop_time%22%2C%22player_names%22%2C%22player_mobiles%22%2C%22id_numbers%22%2C%22system_name%22%2C%22other_id%22%2C%22valid_week%22%2C%22out_system_name%22%2C%22out_other_id%22%2C%22seller_code%22%2C%22status_name%22%2C%22seat_names%22%2C%22goods_name%22%2C%22sign_time%22%2C%22create_time%22%2C%22update_time%22%2C%22pay_mode_name%22%2C%22post_address%22%2C%22operator_id%22%2C%22operator_name%22%2C%22operator_group_name%22%2C%22plate_number%22%2C%22rgoods_id%22%2C%22g_system_remark%22%2C%22commission%22%2C%22r_commission%22%2C%22cate_name%22%2C%22s_wuser_id%22%2C%22s_wuser_account%22%2C%22supplier_group_name%22%2C%22time_slot%22%2C%22shuttle_address%22%2C%22purchase_total%22%2C%22cost_money%22%2C%22cost_spread%22%2C%22invoice_info%22%2C%22booking_info%22%2C%22post_tracking_no%22%5D&g_down=%E4%B8%8B%E8%BD%BD'
        url = 'http://fenxiao.feiyang.cn/Admin/Orders/list.html?g_dselect%5B%5D=orders_id&g_dselect%5B%5D=code&g_dselect%5B%5D=qrcode&g_dselect%5B%5D=remark_name&g_dselect%5B%5D=remark&g_dselect%5B%5D=user_remark_name&g_dselect%5B%5D=user_remark&g_dselect%5B%5D=supplier_remark_name&g_dselect%5B%5D=supplier_remark&g_dselect%5B%5D=goods_id&g_dselect%5B%5D=alias_name&g_dselect%5B%5D=money_send&g_dselect%5B%5D=out_money_send&g_dselect%5B%5D=money_one&g_dselect%5B%5D=money_total&g_dselect%5B%5D=back_cash_one&g_dselect%5B%5D=back_cash_total&g_dselect%5B%5D=purchase_price&g_dselect%5B%5D=user_money_one&g_dselect%5B%5D=user_money_total&g_dselect%5B%5D=profit&g_dselect%5B%5D=profit_total&g_dselect%5B%5D=user_profit&g_dselect%5B%5D=user_profit_total&g_dselect%5B%5D=amount_total&g_dselect%5B%5D=amount_valid&g_dselect%5B%5D=amount_used&g_dselect%5B%5D=amount_apply&g_dselect%5B%5D=amount_refund&g_dselect%5B%5D=is_pay_name&g_dselect%5B%5D=send_type_name&g_dselect%5B%5D=is_send_name&g_dselect%5B%5D=user_id&g_dselect%5B%5D=user_name&g_dselect%5B%5D=group_id&g_dselect%5B%5D=group_name&g_dselect%5B%5D=tgroup_id&g_dselect%5B%5D=tgroup_name&g_dselect%5B%5D=puser_id&g_dselect%5B%5D=puser_name&g_dselect%5B%5D=supplier_id&g_dselect%5B%5D=supplier_name&g_dselect%5B%5D=scenic_name&g_dselect%5B%5D=player_name&g_dselect%5B%5D=player_mobile&g_dselect%5B%5D=id_number&g_dselect%5B%5D=play_date&g_dselect%5B%5D=start_date&g_dselect%5B%5D=expire_date&g_dselect%5B%5D=start_time&g_dselect%5B%5D=stop_time&g_dselect%5B%5D=player_names&g_dselect%5B%5D=player_mobiles&g_dselect%5B%5D=id_numbers&g_dselect%5B%5D=system_name&g_dselect%5B%5D=other_id&g_dselect%5B%5D=valid_week&g_dselect%5B%5D=out_system_name&g_dselect%5B%5D=out_system_name&g_dselect%5B%5D=out_ext_status_name&g_dselect%5B%5D=seller_code&g_dselect%5B%5D=status_name&g_dselect%5B%5D=seat_names&g_dselect%5B%5D=goods_name&g_dselect%5B%5D=sign_time&g_dselect%5B%5D=create_time&g_dselect%5B%5D=update_time&g_dselect%5B%5D=pay_mode_name&g_dselect%5B%5D=post_address&g_dselect%5B%5D=operator_id&g_dselect%5B%5D=operator_name&g_dselect%5B%5D=operator_group_name&g_dselect%5B%5D=plate_number&g_dselect%5B%5D=rgoods_id&g_dselect%5B%5D=g_system_remark&g_dselect%5B%5D=commission&g_dselect%5B%5D=r_commission&g_dselect%5B%5D=cate_name&g_dselect%5B%5D=s_wuser_id&g_dselect%5B%5D=s_wuser_account&g_dselect%5B%5D=supplier_group_name&g_dselect%5B%5D=time_slot&g_dselect%5B%5D=shuttle_address&g_dselect%5B%5D=purchase_total&g_dselect%5B%5D=cost_money&g_dselect%5B%5D=cost_spread&g_dselect%5B%5D=invoice_info&g_dselect%5B%5D=booking_info&g_dselect%5B%5D=post_tracking_no&g_dselect%5B%5D=rebabt_pay&g_dselect%5B%5D=rebabt_deduct&g_dselect%5B%5D=balance_deduct&g_dselect%5B%5D=wuser_info&status=&pay_type=&is_pay=&is_send=&is_export=&is_used=&goods_type=&is_expire=&remark_id=&g_stype=&search_type=&user_id_type=&rgoods_type=&group_id=91103&system=&out_system=&out_ext_status=&d_expire_time=&search=&begin_sign_time=&end_play_time=&end_sign_time=&begin_play_time=&user_id=&supplier_id=&goods_id=&orders_type=&is_booking=&scenic_id=&shoukuan_status=&fukuan_status=&duizhang_status=&g_dfields=%5B%22orders_id%22%2C%22code%22%2C%22qrcode%22%2C%22remark_name%22%2C%22remark%22%2C%22user_remark_name%22%2C%22user_remark%22%2C%22supplier_remark_name%22%2C%22supplier_remark%22%2C%22goods_id%22%2C%22alias_name%22%2C%22money_send%22%2C%22out_money_send%22%2C%22money_one%22%2C%22money_total%22%2C%22back_cash_one%22%2C%22back_cash_total%22%2C%22purchase_price%22%2C%22user_money_one%22%2C%22user_money_total%22%2C%22profit%22%2C%22profit_total%22%2C%22user_profit%22%2C%22user_profit_total%22%2C%22amount_total%22%2C%22amount_valid%22%2C%22amount_used%22%2C%22amount_apply%22%2C%22amount_refund%22%2C%22is_pay_name%22%2C%22send_type_name%22%2C%22is_send_name%22%2C%22user_id%22%2C%22user_name%22%2C%22group_id%22%2C%22group_name%22%2C%22tgroup_id%22%2C%22tgroup_name%22%2C%22puser_id%22%2C%22puser_name%22%2C%22supplier_id%22%2C%22supplier_name%22%2C%22scenic_name%22%2C%22player_name%22%2C%22player_mobile%22%2C%22id_number%22%2C%22play_date%22%2C%22start_date%22%2C%22expire_date%22%2C%22start_time%22%2C%22stop_time%22%2C%22player_names%22%2C%22player_mobiles%22%2C%22id_numbers%22%2C%22system_name%22%2C%22other_id%22%2C%22valid_week%22%2C%22out_system_name%22%2C%22out_system_name%22%2C%22out_ext_status_name%22%2C%22seller_code%22%2C%22status_name%22%2C%22seat_names%22%2C%22goods_name%22%2C%22sign_time%22%2C%22create_time%22%2C%22update_time%22%2C%22pay_mode_name%22%2C%22post_address%22%2C%22operator_id%22%2C%22operator_name%22%2C%22operator_group_name%22%2C%22plate_number%22%2C%22rgoods_id%22%2C%22g_system_remark%22%2C%22commission%22%2C%22r_commission%22%2C%22cate_name%22%2C%22s_wuser_id%22%2C%22s_wuser_account%22%2C%22supplier_group_name%22%2C%22time_slot%22%2C%22shuttle_address%22%2C%22purchase_total%22%2C%22cost_money%22%2C%22cost_spread%22%2C%22invoice_info%22%2C%22booking_info%22%2C%22post_tracking_no%22%2C%22rebabt_pay%22%2C%22rebabt_deduct%22%2C%22balance_deduct%22%2C%22wuser_info%22%5D&g_down=%E4%B8%8B%E8%BD%BD'

        begin_time = '&begin_time=' + str(self.year_before) + '%2F' + str(str(self.month_before).zfill(2)) + '%2F' + str(str(self.day_before).zfill(
            2)) + '+' + str(str(self.hour_before).zfill(2)) + '%3A' + str(str(self.minute_before).zfill(2)) + '%3A' + str(
            str(self.second_before).zfill(2))

        # begin_time = '&begin_time=2020%2F02%2F17+00%3A00%3A00'
        # end_time = begin_time + '&end_time=' + '2020%2F02%2F17+23%3A59%3A59'
        # begin_time = '&begin_time=2020%2F02%2F20+17%3A00%3A00'

        end_time = begin_time + '&end_time=' + str(int(self.year)+1)+'%2F02%2F19+23%3A59%3A59'
        # url = url + '&begin_time=2020%2F02%2F19+22%3A00%3A00&end_time=2020%2F02%2F19+23%3A59%3A59'
        url = url + end_time

        cookie = "g_theme=; Orders_list=remark%2Cnumber%2Cback_cash_one%2Cback_cash_total%2Cexport_code%2Cuser_remark_name%2Coperator%2Csupplier_name%2Cbooking_time%2Cpay_mode_name%2Cinvoice_info%2Cic_info%2Cpost_info; PHPSESSID=" + str(self.PHPSESSID) + "; login_num=0; __root_domain_v=.feiyang.cn; _qddaz=QD.wlzzuy.c8wl04.k6tehkcq; lost_notice_time=1581955200000; last_bulletin_id=124370; _qdda=3-1.3sjjf; _qddab=3-keyuyx.k6tehkec"

        herder = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8",
            "Accept-Encoding": "gzip, deflate, sdch",
            # "Cookie": "g_theme=; Orders_list=remark%2Cnumber%2Cback_cash_one%2Cback_cash_total%2Cexport_code%2Cuser_remark_name%2Coperator%2Csupplier_name%2Cbooking_time%2Cpay_mode_name%2Cinvoice_info%2Cic_info%2Cpost_info; PHPSESSID=1cbeefbbec141f99ab9c9ca295e387444796; login_num=0; __root_domain_v=.feiyang.cn; _qddaz=QD.wlzzuy.c8wl04.k6tehkcq; lost_notice_time=1581955200000; last_bulletin_id=124370; _qdda=3-1.3sjjf; _qddab=3-keyuyx.k6tehkec",
            "Cookie": cookie,
            # "Cookie": "g_theme=; Orders_list=remark%2Cnumber%2Cback_cash_one%2Cback_cash_total%2Cexport_code%2Cuser_remark_name%2Coperator%2Csupplier_name%2Cbooking_time%2Cpay_mode_name%2Cinvoice_info%2Cic_info%2Cpost_info; __root_domain_v=.feiyang.cn; PHPSESSID=13b1fe4b443c26df01f072263e3f4a524797; login_num=0; lost_notice_time=1581955200000; last_bulletin_id=124370; _qddaz=QD.wlzzuy.c8wl04.k6tehkcq; _qddab=3-11el5v.k6u6skdk",
            "Connection": "keep-alive"
        }

        r = requests.get(url, headers=herder)

        # open打开excel文件，报存为后缀为xls的文件
        fp = open("ts_order.html", "wb")
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
    OrderImport = OrderImport()

    try:
        OrderImport.main()
    except BaseException as err:
        traceback.print_exc()
        print('__main__ exception: ')
        print(err)
    finally:
        elapsed = (time.perf_counter() - start)
        print("Python Complete, Time used:", elapsed)
