# coding:utf-8
# coding: utf-8
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from NNW.import_main import ImportMain
from pymongo import UpdateOne
from NNW.import_superior_id import ImportSuperiorId
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


class ImportOrderCustomerId(ImportMain):
    def __init__(self):
        super().__init__()

        self.customer_id_PHPSESSID = self.get_customer_id_PHPSESSID()

        self.oid = 0
        self.cid = 0

        self.OrderTimeISODate_begin = cst_tz.localize(datetime.datetime(2020, 2, 27, 17, 0, 0))

    def main(self):
        # self.control()
        self.update_belong()

        # while True:
        #     print(datetime.datetime.now())
        #     self.control()
        #     print(datetime.datetime.now())
        # 
        #     print('运行结束')
        #     time.sleep(30)

    def control(self):
        print('start ImportOrderCustomerId control')

        i = 0
        while True:
            i += 1
            print(str(i), '开始')
            url = 'http://fenxiao.feiyang.cn/Admin/Orders/myWebList.html?status=1&pay_type=&is_send=&is_export=&is_used=&rgoods_type=&is_expire=&d_expire_time=0%E5%A4%A900%3A00%3A00&operator_id=&is_booking=&goods_type=&is_free_sheet=&g_web_group_id=&pay_mode=&search_type=g_auto&search=&scenic_name=&scenic_id=&goods_name=&goods_id=&account=&wuser_id=&begin_play_time=&end_play_time=&begin_sign_time=&end_sign_time=&orders_type=0&dosubmit=%E6%90%9C%E7%B4%A2'
            begin_time = '&begin_time=' + str(self.year_before) + '%2F' + str(
                str(self.month_before).zfill(2)) + '%2F' + str(str(self.day_before).zfill(
                2)) + '+' + str(str(self.hour_before).zfill(2)) + '%3A' + str(
                str(self.minute_before).zfill(2)) + '%3A' + str(
                str(self.second).zfill(2))

            end_time = begin_time + '&end_time=' + str(int(self.year) + 1) + '%2F02%2F19+23%3A59%3A59'

            url = url + end_time
            url = url + '&limit=200&page=' + str(i)

            self.download_html(url)
            res = self.read_html()

            if not res:
                break

        ISI = ImportSuperiorId()
        ISI.control()

        self.update_belong()

    def get_customer_id_PHPSESSID(self):
        condition = {
            'String': 'CustomerIdTSPHPSESSID',
        }

        data = self.col_CustomSettings.find_one(condition)

        return data['PHPSESSID']

    def get_superior(self):
        temp_condition = {
            'DelStatus': 0,
        }

        res = self.col_NNWCustomer.find(temp_condition)

        self.superior = {}

        for one in res:
            self.superior[one['ID']] = one['shang4_ji2_ID']

    def get_id_name(self):
        temp_condition = {
            'DelStatus': 0,
        }

        res = self.col_NNWIdName.find(temp_condition)

        return res

    def update_belong(self):
        print('start update_belong')
        old_object = {}
        id_name_list = self.get_id_name()
        self.get_superior()

        for one in id_name_list:
            old_object[str(one['ID'])] = {
                'ID': str(one['ID']),
                'RealName': str(one['RealName']),
                'Group': str(one['Group']),
                'GroupId': int(one['GroupId']),
                'GroupClass': int(one['GroupClass']),
            }

        self.old_object = old_object

        temp_project = {
            'OrderCustomerId': 1,
            'ShareData': 1,
            'fen1_xiang3_yuan2_ID': 1,
            'yong4_hu4_bei4_zhu4': 1,
        }

        # temp_condition = {#更新没有归属人id的订单
        #     'DelStatus': 0,
        #     'OrderCustomerId': {'$exists': True},
        #     "$or": [
        #         {'BelongId': {'$exists': False}},
        #         {'BelongId': ''}
        #     ]
        # }

        # 更新所有 下级归属 订单
        # temp_condition = {
        #     'DelStatus': 0,
        #     'OrderCustomerId': {'$exists': True},
        #     'JudgeBelongType.TypeId': {"$in":[3,6]},
        # }

        # temp_condition = {
        #     'DelStatus': 0,
        #     'OrderCustomerId': {'$exists': True},
        #     'OrderTimeISODate': {
        #         '$gte': self.OrderTimeISODate_begin,
        #     },
        # }

        # 新增员工修正归属人
        # pid = '30140483'
        # temp_condition = {
        #     'OrderCustomerId': {'$exists': True},
        #     'DelStatus': 0,
        #     "$or": [{"fen1_xiang3_yuan2_ID": pid}, {"OrderCustomerId": pid},{'JudgeBelongType.TypeId': {'$in': [3, 6]}}]
        # }

        # 更新无归属
        # temp_condition = {
        #     'DelStatus': 0,
        #     "BelongName": "",
        #     'OrderCustomerId': {'$exists': True},
            # 'BelongId': {'$exists': False},
        # }

        lookup = {
            'from': "NNWIdName",
            "localField": "fen1_xiang3_yuan2_ID",
            "foreignField": "ID",
            "as": "ShareData",
        }

        list = self.col_NNWOrder.aggregate(
            [
                {'$match': temp_condition},
                {'$lookup': lookup},
                {'$project': temp_project},
            ]
        )

        bulk_op = []

        for one in list:
            update_data = {}
            update_data['BelongName'] = ''
            update_data['BelongId'] = ''
            update_data['BelongGroup'] = ''

            try:
                update_data['BelongName'] = one['ShareData'][0]['RealName']
                update_data['BelongId'] = one['ShareData'][0]['ID']
                update_data['BelongGroup'] = one['ShareData'][0]['Group']
                update_data['BelongGroupId'] = one['ShareData'][0]['GroupId']
                update_data['BelongGroupClass'] = one['ShareData'][0]['GroupClass']
                update_data['JudgeBelongType'] = {
                    "TypeId": 2,
                    # "Type": "员工分享",
                    "Type": "员工分享：客户扫码下单",
                    "Remark": ""
                }
            except:
                pass

            if '何总客人' in one['yong4_hu4_bei4_zhu4']:
                update_data['BelongName'] = '何斌锋'
                update_data['BelongId'] = '29216099'
                update_data['BelongGroup'] = '总经办'
                update_data['BelongGroupId'] = 1
                update_data['BelongGroupClass'] = 1
                update_data['JudgeBelongType'] = {
                    "TypeId": 4,
                    "Type": "何总客人",
                    "Remark": ""
                }
            elif 'OrderCustomerId' in one:
                if one['OrderCustomerId'] in old_object:
                    temp_index = str(one['OrderCustomerId'])
                    update_data['BelongName'] = old_object[temp_index]['RealName']
                    update_data['BelongId'] = old_object[temp_index]['ID']
                    update_data['BelongGroup'] = old_object[temp_index]['Group']
                    update_data['BelongGroupId'] = old_object[temp_index]['GroupId']
                    update_data['BelongGroupClass'] = old_object[temp_index]['GroupClass']
                    update_data['JudgeBelongType'] = {
                        "TypeId": 1,
                        # "Type": "员工下单",
                        "Type": "员工下单：销售代下单",
                        "Remark": ""
                    }
                if not update_data['BelongName']:
                    temp_res = self.find_superior(one['OrderCustomerId'])

                    if temp_res:
                        temp_index = str(temp_res)
                        update_data['BelongName'] = old_object[temp_index]['RealName']
                        update_data['BelongId'] = old_object[temp_index]['ID']
                        update_data['BelongGroup'] = old_object[temp_index]['Group']
                        update_data['BelongGroupId'] = old_object[temp_index]['GroupId']
                        update_data['BelongGroupClass'] = old_object[temp_index]['GroupClass']
                        update_data['JudgeBelongType'] = {
                            "TypeId": 3,
                            # "Type": "员工下级",
                            "Type": "员工下级：客户商城下单",
                            "Remark": ""
                        }

            try:
                if not update_data['BelongName'] and one['fen1_xiang3_yuan2_ID']:
                    temp_res = self.find_superior(one['fen1_xiang3_yuan2_ID'])

                    if temp_res:
                        temp_index = str(temp_res)
                        update_data['BelongName'] = old_object[temp_index]['RealName']
                        update_data['BelongId'] = old_object[temp_index]['ID']
                        update_data['BelongGroup'] = old_object[temp_index]['Group']
                        update_data['BelongGroupId'] = old_object[temp_index]['GroupId']
                        update_data['BelongGroupClass'] = old_object[temp_index]['GroupClass']
                        update_data['JudgeBelongType'] = {
                            "TypeId": 6,
                            # "Type": "下级分享",
                            "Type": "下级分享：客户扫码下单",
                            "Remark": ""
                        }
            except:
                pass

            temp_one = UpdateOne(
                {
                    '_id': one['_id'],
                },
                {
                    '$set': update_data
                },
            )
            bulk_op.append(temp_one)

        if len(bulk_op) > 0:
            self.col_NNWOrder.bulk_write(bulk_op)

    def find_superior(self, id):
        if id in self.superior:
            if not self.superior[str(id)]:
                return False
            if str(self.superior[str(id)]) in self.old_object:
                return str(self.superior[str(id)])

            return self.find_superior(str(self.superior[str(id)]))

        return False

    def read_html(self):
        print('start read_html')
        path = self.file_path + "ts_order_customer_id.html"
        # path = "D:\\python_work_erp\\NNW\\ts_order_customer_id.html"
        # path = "/project/erp/python_work/NNW/ts_order.html"

        with open(path, 'r', encoding="utf-8") as f:
            Soup = bs4.BeautifulSoup(f.read(), 'html.parser')

            try:
                get_divs = Soup.select('tbody tr td h4')
                for d in get_divs:
                    if '暂无数据' in d:
                        print('暂无数据')
                        return False
            except:
                pass

            get_divs = Soup.select('div')
            for d in get_divs:
                if '用户登录过期' in d:
                    print('用户登录过期')
                    return False

            get_values = Soup.select('body div div div table tbody tr td')

        res = []

        i = 0
        for v in get_values:
            temp_v = str(v)
            if '<td align="center" class="id">' in temp_v:
                i = 1
                self.oid = str(v.string)
            else:
                i += 1

            if i == 22:
                v = str(v)
                v = v.split('<br/>')
                self.cid = v[0].split('class="">')[1]

                res.append({
                    'oid': str(self.oid),
                    'cid': str(self.cid),
                })

        self.import_data(res)

        return True

    def import_data(self, res):
        print('start import_data')
        bulk_op = []

        for one in res:
            temp_one = UpdateOne(
                {
                    'ID': str(one['oid']),
                    'OrderCustomerId': {'$exists': False},
                },
                {
                    '$set': {
                        'OrderCustomerId': str(one['cid'])
                    }
                },
            )
            bulk_op.append(temp_one)

        if len(bulk_op) > 0:
            self.col_NNWOrder.bulk_write(bulk_op)

    def download_html(self, url):
        print('start download_html')
        cookie = '__root_domain_v=.feiyang.cn; lost_notice_time=1581955200000; _qddamta_2885813712=3-0; Orders_list=remark%2Cwuser_info%2Cnumber%2Cback_cash_one%2Cback_cash_total%2Camount_taked%2Cgroup_name%2Csupplier%2Cremark_name%2Cuser_remark%2Coperator%2Coperator_group_name%2Cbooking_time%2Cplate_number%2Cpost_info%2Cconf_name%2Cpay_mode_s%2Cinvoice_info%2Cbooking_info%2Cic_info%2Csubweb_info; g_theme=Classical; _qddaz=QD.bnkw2h.x72sv6.k6j1ja8l; _qdda=3-1.2jffc1; _qddab=3-lmn3vv.k6x2zn5u; PHPSESSID=' + self.customer_id_PHPSESSID + '; login_num=0'

        herder = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.8",
            "Accept-Encoding": "gzip, deflate, sdch",
            "Cookie": cookie,
            "Connection": "keep-alive"
        }

        r = requests.get(url, headers=herder)

        fp = open("ts_order_customer_id.html", "wb")
        fp.write(r.content)
        fp.close()


if __name__ == '__main__':
    start = time.perf_counter()
    ImportOrderCustomerId = ImportOrderCustomerId()

    try:
        ImportOrderCustomerId.main()
    except BaseException as err:
        traceback.print_exc()
        print('__main__ exception: ')
        print(err)
    finally:
        elapsed = (time.perf_counter() - start)
        print("Python Complete, Time used:", elapsed)
