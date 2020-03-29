# Title : 定期同步飞猪产品至erp

# coding=UTF-8
# !/usr/bin/python3
import copy
import _thread
import json
import os
import random
import urllib
from dateutil import parser
import pymongo
import datetime
import time
import socket
import requests
from bson import json_util
from pymongo import ReturnDocument, UpdateOne, InsertOne
import sys
import mongodb_config
from bson.objectid import ObjectId
from pytz import utc
from pytz import timezone
from constants import const
import hashlib
import _md5
import top.api
from constants import const
import _thread
import threading

# 时区
cst_tz = timezone('Asia/Shanghai')

year = datetime.datetime.now().year
month = datetime.datetime.now().month
day = datetime.datetime.now().day
hour = datetime.datetime.now().hour
minute = datetime.datetime.now().minute
second = datetime.datetime.now().second
# now_iso_date = datetime.datetime(year, month, day, hour, minute, second).replace(tzinfo=cst_tz)
now_iso_date = cst_tz.localize(datetime.datetime(year, month, day, hour, minute, second))
now_timestamp = int(time.time())
sync_no = int(round(time.time() * 1000))
now_str = (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

today = datetime.date.today()
yesterday_end_time = int(time.mktime(time.strptime(str(today), '%Y-%m-%d'))) - 1
# 今天开始时间戳
today_start_time = yesterday_end_time + 1

lc_base_url = 'http://80yrm8bm30.api.taobao.com/router/qmtest'
main_base_url = 'http://11186295n9.api.taobao.com/router/qmtest'
# lc_base_url = 'http://80yrm8bm30.api.taobao.com/router/qm'
# main_base_url = 'http://11186295n9.api.taobao.com/router/qm'
partner_id = 'taobao-sdk-python-20190912'

const_appkey_main = '27761322'
const_secret_main = '98e36ac9fa56d9c5af04a6777a67d7af'
const_sessionkey_main = ''
score_obj_saler = '58d9c05f22393b87b081b23c'
pre_BasicsGADDRData = {}
pre_plan_price_list = {}
pre_plan_list = {}

const_AddInfo = {
    'ForeignKeyID': ObjectId('000000000000000000002251'),
    'EmployeeName': '系统管理员Online',
    'EmployeeDepartmentID': ObjectId('000000000000000000000781'),
    'EmployeeDepartmentName': '浙江恒越信息科技有限公司',
}
const_CompanyInfo = {
    'ForeignKeyID': ObjectId('000000000000000000000001'),
    'DelStatus': 0,
    'CompanyName': '浙江飞扬国际旅游集团股份有限公司',
    'ParentCompanyID': ObjectId('000000000000000000000000'),
    'CompanyNameSimplifiedSpelling': 'FY',
    'CompanyDefaultContact': '何斌锋',
    'CompanyDefaultContactMobile': '',
    'CompanyStatus': 1,
}
const_product_type = {
    'ForeignKeyID': ObjectId('000000000000000000000037'),
    'ProductTypeName': '三方平台',
    'IsEnableFunction': True,
}
const_independent_travel_businessTypeID = '5c061966c98f8128008b4574'
const_independent_travel_businessType = {
    'id': 2,
    'name': '自由行',
    'sub_type': {
        'id': 1,
        'name': '机+酒',
    },
}
const_special_remark = 'K23'

if __name__ == "__main__":
    mongodb = mongodb_config.get_mongodb_config()
    mongodb_ip = mongodb['mongodb_ip']
    mongodb_port = mongodb['mongodb_port']
    mongodb_auth = mongodb['mongodb_auth']
    mongodb_password = mongodb['mongodb_password']

# 连接MongoDB
client = pymongo.MongoClient(mongodb_ip, mongodb_port)
db = client.erp
db.authenticate(mongodb_auth, mongodb_password)

col_qualityscore = db.QualityScore
col_erpRemind = db.erpRemind
col_ProductTeamtour = db.ProductTeamtour
col_BasicsGADDR = db.BasicsGADDR
col_businessType = db.businessType
col_ProductPlan = db.ProductPlan
col_ProductPlanPrice = db.ProductPlanPrice
col_OrderClaimConfiguration = db.OrderClaimConfiguration
col_AliFliggyStores = db.AliFliggyStores
col_ChangeLog = db.ChangeLog
col_ids = db.ids


def isoformat(time):
    '''
    将datetime或者timedelta对象转换成ISO 8601时间标准格式字符串
    :param time: 给定datetime或者timedelta
    :return: 根据ISO 8601时间标准格式进行输出
    '''
    if isinstance(time, datetime.datetime):  # 如果输入是datetime
        return time.isoformat()
    elif isinstance(time, datetime.timedelta):  # 如果输入时timedelta，计算其代表的时分秒
        hours = time.seconds // 3600
        minutes = time.seconds % 3600 // 60
        seconds = time.seconds % 3600 % 60
        return 'P%sDT%sH%sM%sS' % (time.days, hours, minutes, seconds)  # 将字符串进行连接


class sync:
    def main(self, threadName, goods):
        try:
            num_iid = goods['num_iid']
            # num_iid = '596763462698'
            # num_iid = '598145720467'

            if const_need_print:
                print('num_iid: ', num_iid, 'main 处理开始')

            outer_id_exists = True
            if 'outer_id' not in goods:
                outer_id_exists = False

            if not goods['outer_id']:
                outer_id_exists = False

            if not outer_id_exists:
                log = {
                    'Function': 'main',
                    'Subtype': 9,
                    'Message': 'num_iid: ' + str(num_iid) + ' outer_id 字段不存在, 跳过处理',
                    'Data': goods,
                }
                sync.add_log(log)
                return False

            temp_res = sync.single_query(num_iid, 0)

            if 'alitrip_travel_item_single_query_response' not in temp_res:
                # 记录日志
                if const_need_print:
                    print('main: num_iid: ' + str(num_iid) + ' alitrip_travel_item_single_query_response 字段不存在')

                log = {
                    'Function': 'main',
                    'Subtype': 2,
                    'Message': 'main: num_iid: ' + str(num_iid) + ' alitrip_travel_item_single_query_response 字段不存在',
                    'Result': temp_res,
                }
                sync.add_log(log)
                return False

            temp_res = temp_res['alitrip_travel_item_single_query_response']

            if 'travel_item' not in temp_res:
                # 记录日志
                log = {
                    'Function': 'main',
                    'Subtype': 3,
                    'Message': 'main: num_iid: ' + str(num_iid) + ' travel_item 字段不存在',
                    'Result': temp_res,
                }
                sync.add_log(log)
                return False

            if not temp_res['travel_item']:
                # 记录日志
                log = {
                    'Function': 'main',
                    'Subtype': 4,
                    'Message': 'main: num_iid: ' + str(num_iid) + ' travel_item 字段为空',
                    'Result': temp_res,
                }
                sync.add_log(log)
                return False

            if 'out_id' not in temp_res['travel_item']['base_info']:
                # 记录日志
                log = {
                    'Function': 'main',
                    'Subtype': 5,
                    'Message': 'main: num_iid: ' + str(num_iid) + ' out_id 字段不存在, 跳过处理',
                    'Result': temp_res,
                }
                sync.add_log(log)
                return False

            if not temp_res['travel_item']['base_info']['out_id']:
                # 记录日志
                log = {
                    'Function': 'main',
                    'Subtype': 6,
                    'Message': 'main: num_iid: ' + str(num_iid) + ' out_id 字段为空, 跳过处理',
                    'Result': temp_res,
                }
                sync.add_log(log)
                return False

            detail = temp_res['travel_item']
            detail['base_info']['out_id'] = str(detail['base_info']['out_id'])
            detail['item_id'] = str(detail['item_id'])

            try:
                detail['base_info'].pop('desc')
            except BaseException as err:
                pass

            try:
                detail['freedom_item_ext'].pop('other_infos')
            except BaseException as err:
                pass

            try:
                detail['freedom_item_ext'].pop('traffic_desc')
            except BaseException as err:
                pass

            try:
                detail.pop('refund_info')
            except BaseException as err:
                pass

            try:
                detail.pop('sale_info')
            except BaseException as err:
                pass

            ################
            ################
            # 有效操作开始
            ################
            ################

            # vision_no = sync.set_vision_no(detail['base_info']['out_id'])

            try:
                # 有时候是pontus_travel_item_sku_info 有时候是 item_sku_info
                if 'pontus_travel_item_sku_info' not in detail['sku_infos'] and 'item_sku_info' not in detail[
                    'sku_infos']:
                    return False

                sku_infos = {}
                try:
                    if detail['sku_infos']['pontus_travel_item_sku_info']:
                        sku_infos = detail['sku_infos']['pontus_travel_item_sku_info']
                except BaseException as err:
                    pass

                try:
                    if detail['sku_infos']['item_sku_info']:
                        sku_infos = detail['sku_infos']['item_sku_info']
                except BaseException as err:
                    pass

                # 检查outer_sku_id 是否存在重复
                if not sync.check_outer_sku_id_repeat(detail, sku_infos):
                    return False

                # 预先获取出发地/目的地数据
                sync.get_pre_BasicsGADDRData(sku_infos)

                if 'package_name' in sku_infos:
                    # 存在sku信息不是list 格式的情况
                    sku_infos = [
                        sku_infos
                    ]

                for sku_info in sku_infos:
                    sync.do_create_product(detail, sku_info, list_res['approve_status_list'][str(num_iid)])

                temp_condition = {
                    'IsDel': 0,
                    'AliField.item_id': str(num_iid),
                    'syncNo': sync_no,
                }
                temp_project = {
                    'ProductType': 1,
                    'ProductNo': 1,
                    'Title': 1,
                    'ProductGADDR': 1,
                    'AliField': 1,
                    'Belong': 1,
                }
                operated_product_list = col_ProductTeamtour.find(temp_condition, temp_project)

                temp_condition = {
                    'IsDel': 0,
                    'fliggy_item_id': str(num_iid),
                }
                temp_project = {
                    'ProductNo': 1,
                    'ProductID': 1,
                    'PlanNumber': 1,
                    'MorethanNumber': 1,
                    'Title': 1,
                    'ProductGADDR': 1,
                    'StartTime': 1,
                    'fliggy_item_id': 1,
                    'fliggy_outer_sku_id': 1,
                    'fliggy_date': 1,
                    'fliggy_price': 1,
                    'fliggy_price_type': 1,
                    'fliggy_stock': 1,
                    'fliggy_package_name': 1,
                    'fliggy_package_desc': 1,
                }
                plan_list = col_ProductPlan.find(temp_condition, temp_project)

                global pre_plan_list

                for plan_one in plan_list:
                    index = str(plan_one['fliggy_outer_sku_id']) + str(plan_one['fliggy_date'])
                    pre_plan_list[index] = plan_one

                for product_data in operated_product_list:
                    product_data['ProductName'] = product_data.pop('Title')
                    sku_info = product_data['AliField']['sku_info']

                    sync.do_create_plan(sku_info, detail, product_data, pre_plan_list)

                # 批量创建价格方案
                sync.create_plan_price_batch(num_iid)

            except BaseException as err:
                if const_need_print:
                    print('Exception main deep: ', err)
                # 记录日志

            # sync.del_unrelated_product(num_iid)

            # 删除无关联的计划
            sync.del_unrelated_plan('', num_iid)

            sync.update_plan_count_all(num_iid)
            # 删除无关价格方案
            sync.del_unrelated_planprice('', num_iid)

            # 更新单房差, 把单房差更新到价格方案中
            sync.update_singleroomdiff(detail['item_id'])

            log = {
                'Function': 'main',
                'Subtype': 7,
                'Message': 'main: num_iid: ' + str(num_iid) + ' 处理成功',
                'Result': temp_res,
            }
            sync.add_log(log)

        except BaseException as err:
            log = {
                'Function': 'main',
                'Subtype': 8,
                'Message': 'Exception main: ' + str(err),
            }
            sync.add_log(log)
        finally:
            return 'success'

    # 批量创建价格方案
    def create_plan_price_batch(self, num_iid):
        global pre_plan_price_list

        temp_condition = {
            'IsDel': 0,
            'fliggy_item_id': str(num_iid),
            'syncNo': sync_no,
        }
        temp_project = {
            'ProductID': 1,
            'PlanNumber': 1,
            'MorethanNumber': 1,
            'fliggy_item_id': 1,
            'fliggy_outer_sku_id': 1,
            'fliggy_date': 1,
            'fliggy_price': 1,
            'fliggy_price_type': 1,
            'fliggy_stock': 1,
            'fliggy_package_name': 1,
            'fliggy_package_desc': 1,
        }
        plan_list = col_ProductPlan.find(temp_condition, temp_project)

        temp_condition = {
            'IsDel': 0,
            'fliggy_item_id': str(num_iid),
        }
        temp_project = {
            'PlanID': 1,
            'fliggy_date': 1,
            'fliggy_outer_sku_id': 1,
        }
        res = col_ProductPlanPrice.find(temp_condition, temp_project)

        for price in res:
            index = str(price['fliggy_outer_sku_id']) + str(price['fliggy_date'])
            pre_plan_price_list[index] = price

        ns = []
        for plan_data in plan_list:
            temp_res = sync.create_plan_price_bulk(plan_data, num_iid, pre_plan_price_list)

            if temp_res:
                ns.append(temp_res)
        if ns:
            col_ProductPlanPrice.bulk_write(ns)

    # 预先获取出发地/目的地数据
    def get_pre_BasicsGADDRData(self, sku_infos):
        global pre_BasicsGADDRData

        title_list = ['宁波', '日本']

        for sku_info in sku_infos:
            combos = sku_info['combos']
            combos = json.loads(combos)

            if 'comboName' not in combos:
                combos = combos[0]

            if 'c' not in combos['from']:
                from_add = combos['from'][0]
            else:
                from_add = combos['from']

            c = from_add['c']
            c = c.split(':')[1]

            if 'cy' not in combos['to']:
                to_add = combos['to'][0]
            else:
                to_add = combos['to']

            cy = to_add['cy']
            cy = cy.split(':')[1]

            title_list.append(cy)
            title_list.append(c)

        temp_condition = {
            'DelStatus': 0,
            'IsDestination': True,
            'Title': {'$in': title_list},
        }

        temp_project = {
            'Title': 1,
            'TitleEN': 1,
            'parentid': 1,
            'ParentName': 1,
            'Shorter': 1,
            'Parents': 1,
        }

        BasicsGADDR = col_BasicsGADDR.find(temp_condition, temp_project)

        for one in BasicsGADDR:
            index = str(one['Title'])
            pre_BasicsGADDRData[index] = {
                'ForeignKeyID': ObjectId(one['_id']),
                'DelStatus': 0,
                'Title': one['Title'],
                'TitleEN': one['TitleEN'],
                'ParentID': one['parentid'],
                'ParentName': one['ParentName'],
                'Shorter': one['Shorter'],
                'OrderBy': 0,
                'Parents': one['Parents'],
            }

    # 检查outer_sku_id 是否存在重复
    def check_outer_sku_id_repeat(self, detail, sku_infos):
        temp_list = []
        for sku_info in sku_infos:
            try:
                outer_sku_id = str(sku_info['outer_sku_id'])

                if outer_sku_id in temp_list:
                    log = {
                        'Function': 'main',
                        'Subtype': 10,
                        'Message': '产品id: ' + str(detail['item_id']) + ' ' + str(outer_sku_id) + ' 存在相同套餐编码, 请检查后重试',
                    }
                    sync.add_log(log)

                    return False

                temp_list.append(outer_sku_id)
            except BaseException as err:
                continue

        return True

    # 更新单房差, 把单房差更新到价格方案中
    def update_singleroomdiff(self, item_id):
        temp_condition = {
            'IsDel': 0,
            'AliField.item_id': str(item_id),
            'syncNo': sync_no,
        }
        temp_project = {
            'AliField': 1,
        }
        product_list = col_ProductTeamtour.find(temp_condition, temp_project)

        updates = []
        for product in product_list:
            try:
                prices = {}
                try:
                    if product['AliField']['sku_info']['prices']['pontus_travel_prices']:
                        prices = product['AliField']['sku_info']['prices']['pontus_travel_prices']
                except BaseException as err:
                    pass

                try:
                    if product['AliField']['sku_info']['prices']['prices']:
                        prices = product['AliField']['sku_info']['prices']['prices']
                except BaseException as err:
                    pass

                outer_sku_id = str(product['AliField']['sku_info']['outer_sku_id'])

                if 'date' in prices:
                    prices = [
                        prices
                    ]
            except BaseException as err:
                log = {
                    'Function': 'update_singleroomdiff',
                    'Subtype': 49,
                    'Message': 'Exception update_singleroomdiff: ' + str(err),
                }
                sync.add_log(log)
                return

            for price in prices:
                if int(price['price_type']) == 3:
                    one = UpdateOne(
                        {
                            'IsDel': 0,
                            'fliggy_item_id': str(item_id),
                            'fliggy_outer_sku_id': str(outer_sku_id),
                            'fliggy_date': str(price['date']),
                        },
                        {
                            '$set': {
                                'SingleRoomDiff': float(int(price['price']) / 100),
                                'UpdateTime': now_iso_date,
                            }
                        },
                    )
                    updates.append(one)

                    # col_ProductPlanPrice.update_one(
                    #     {
                    #         'IsDel': 0,
                    #         'AliField.item_id': str(item_id),
                    #         'AliField.outer_sku_id': str(outer_sku_id),
                    #         'AliField.date': str(price['date']),
                    #     },
                    #     {
                    #         "$set": {
                    #             'SingleRoomDiff': float(int(price['price']) / 100),
                    #             'UpdateTime': now_iso_date,
                    #         },
                    #     }
                    # )

                    # updates.append(
                    #     {
                    #         'q': {
                    #             'IsDel': 0,
                    #             'AliField.item_id': str(item_id),
                    #             'AliField.outer_sku_id': str(outer_sku_id),
                    #             'AliField.date': str(price['date']),
                    #         },
                    #         'u': {
                    #             '$set': {
                    #                 'SingleRoomDiff': float(price['price'] / 100),
                    #                 'UpdateTime': now_iso_date,
                    #             }
                    #         }
                    #     }
                    # )

        if updates:
            col_ProductPlanPrice.bulk_write(updates)

            # db.command(
            #     {
            #         'update': 'ProductPlanPrice',
            #         'updates': updates,
            #     },
            # )

    # 删除无关联的产品
    def del_unrelated_product(self, num_iid):
        temp_condition = {
            'AliField.item_id': str(num_iid),
            'syncNo': {'$ne': sync_no},
        }

        col_ProductPlan.update_many(
            temp_condition,
            {
                "$set": {
                    "IsDel": 1,
                    "DelTime": now_iso_date,
                    "DelSource": 'DelUnrelatedProduct',
                }
            },
        )

    # 设置 视线编号
    def set_vision_no(self, out_id):
        temp_condition = {
            'AliField.base_info.out_id': str(out_id),
            'VisionNo': {'$exists': True},
        }

        temp_project = {
            'VisionNo': 1,
        }

        temp_res = col_ProductTeamtour.find_one(temp_condition, temp_project)

        if temp_res:
            vision_no = temp_res['VisionNo']
        else:
            list = col_ProductTeamtour.aggregate(
                [
                    {
                        '$match': {
                            'IsDel': 0,
                            'VisionNo': {'$exists': True},
                        },
                    },
                    {
                        '$project':
                            {
                                'VisionNo': 1,
                            }
                    },
                    {
                        '$sort': {'VisionNo': -1}
                    },
                    {
                        '$limit': 1
                    },
                ]
            )

            if not list:
                start_no = 1
            else:
                for one in list:
                    start_no = int(one['VisionNo'][10:15]) + 1

            vision_no = 'FY' + str('%04d' % year) + str('%02d' % month) + str('%02d' % day) + str('%05d' % start_no)

        return vision_no

    # 开始操作创建产品
    def do_create_product(self, detail, sku_info, approve_status):
        if 'outer_sku_id' not in sku_info:
            return

        if not sku_info['outer_sku_id']:
            # 记录日志
            return

        sku_info['outer_sku_id'] = str(sku_info['outer_sku_id'])
        create_product_res = sync.create_product(detail, sku_info, approve_status)

        # sync.do_create_plan(sku_info, detail, create_product_res)

        # 删除无关联的计划
        # sync.del_unrelated_plan(create_product_res, create_product_res['AliField']['item_id'])

        # sync.update_plan_count(create_product_res)

    # 删除无关联的计划
    def del_unrelated_plan(self, product_data='', item_id=''):
        temp_condition = {
            'fliggy_item_id': item_id,
            'syncNo': {'$ne': sync_no},
        }

        try:
            if product_data['ProductNo']:
                temp_condition['ProductNo'] = product_data['ProductNo']
        except BaseException as err:
            pass

        col_ProductPlan.update_many(
            temp_condition,
            {
                "$set": {
                    "IsDel": 1,
                    "DelTime": now_iso_date,
                    "DelSource": 'DelUnrelatedPlan',
                }
            },
        )

    # 更新产品的计划数量
    def update_plan_count(self, product_data):
        temp_condition = {
            'IsDel': 0,
            'ProductNo': str(product_data['ProductNo']),
        }
        total_count = col_ProductPlan.count_documents(temp_condition)

        temp_condition = {
            'IsDel': 0,
            'ProductNo': str(product_data['ProductNo']),
            'StartTime': {
                '$gte': now_iso_date,
            }
        }
        valid_count = col_ProductPlan.count_documents(temp_condition)

        col_ProductTeamtour.update_one(
            {
                '_id': ObjectId(product_data['_id'])
            },
            {
                "$set": {
                    'TotalPlanned': total_count,
                    'ValidPlanCount': valid_count,
                },
            }
        )

    # 更新产品的计划数量
    def update_plan_count_all(self, num_iid):
        temp_condition = {
            'IsDel': 0,
            'fliggy_item_id': str(num_iid),
            'syncNo': sync_no,
        }
        temp_project = {
            'ProductID': 1,
            'StartTime': 1,
        }
        temp_res = col_ProductPlan.find(temp_condition, temp_project)

        c_total_count = {}
        c_valid_count = {}

        for plan in temp_res:
            product_id = str(plan['ProductID'])

            if product_id in c_total_count:
                c_total_count[product_id] += 1
            else:
                c_total_count[product_id] = 1

            StartTime = int(time.mktime(plan['StartTime'].timetuple())) + 8 * 60 * 60
            if StartTime >= today_start_time:
                if product_id in c_valid_count:
                    c_valid_count[product_id] += 1
                else:
                    c_valid_count[product_id] = 1

        updates = []
        for key in c_total_count.keys():
            update = {
                'TotalPlanned': c_total_count[key],
                'ValidPlanCount': 0,
            }

            if key in c_valid_count:
                update['ValidPlanCount'] = c_valid_count[key]

            one = UpdateOne(
                {
                    '_id': ObjectId(key)
                },
                {
                    "$set": update,
                }
            )
            updates.append(one)

        if updates:
            col_ProductTeamtour.bulk_write(updates)

    def do_create_plan(self, sku_info, detail, product_data, pre_plan_list):
        try:
            try:
                if sku_info['prices']['pontus_travel_prices']:
                    prices = sku_info['prices']['pontus_travel_prices']
            except BaseException as err:
                pass

            try:
                if sku_info['prices']['prices']:
                    prices = sku_info['prices']['prices']
            except BaseException as err:
                pass

            if 'date' in prices:
                prices = [
                    prices
                ]

            ns = []
            insert_text = []

            i = 0
            for price in prices:
                i += 1

                if 'date' not in price:
                    continue

                res = sync.create_plan(sku_info, detail, product_data, price, pre_plan_list)

                if res != False:
                    # print('进入 type')

                    if res['type'] == 'update':
                        final = UpdateOne(
                            res['filter'],
                            res['update'],
                        )
                        ns.append(final)

                    if res['type'] == 'insert':
                        final = InsertOne(res['document'])

                        ns.append(final)

            if len(ns) > 0:
                col_ProductPlan.bulk_write(ns)

            # if len(insert_text) > 0:
            #     col_ProductPlan.insert_many(insert_text, False)


        except BaseException as err:
            log = {
                'Function': 'do_create_plan',
                'Subtype': 49,
                'Message': 'Exception do_create_plan: ' + str(err),
            }
            sync.add_log(log)
            # 记录日志

    # 创建计划
    def create_plan(self, sku_info, detail, product_data, price, pre_plan_list):
        try:
            # 只添加成人价格方案 如果price价格方案不存在,计划也不会被创建
            try:
                if int(price['price_type']) == 1:
                    pass
                else:
                    return False
            except BaseException as err:
                return False

            product_data['AliField']['pontus_travel_price'] = price

            op_data = sync.set_plan_info(product_data, price, detail)
            op_data['syncNo'] = sync_no

            op_data['fliggy_item_id'] = str(detail['item_id'])
            op_data['fliggy_outer_sku_id'] = str(sku_info['outer_sku_id'])
            op_data['fliggy_package_name'] = str(sku_info['package_name'])
            if 'fliggy_package_desc' in sku_info:
                op_data['fliggy_package_desc'] = str(sku_info['fliggy_package_desc'])
            op_data['fliggy_date'] = str(price['date'])
            op_data['fliggy_price'] = str(price['price'])
            op_data['fliggy_price_type'] = str(price['price_type'])
            op_data['fliggy_stock'] = str(price['stock'])

            index = str(sku_info['outer_sku_id']) + str(price['date'])

            if index in pre_plan_list:
                # erp中已生成计划
                temp_op_data = copy.deepcopy(op_data)

                temp_op_data['UpdateTime'] = now_iso_date
                temp_op_data.pop('ProductNo')
                temp_op_data.pop('ProductID')
                temp_op_data.pop('PlanNumber')
                temp_op_data.pop('MorethanNumber')

                # return UpdateOne(
                #     {
                #         "_id": ObjectId(pre_plan_list[index]['_id'])
                #     },
                #     {
                #         "$set": temp_op_data
                #     },
                # )

                return {
                    'type': 'update',
                    'filter': {
                        "_id": ObjectId(pre_plan_list[index]['_id'])
                    },
                    'update': {
                        "$set": temp_op_data
                    },
                }
                col_ProductPlan.update_one(
                    {
                        '_id': ObjectId(temp_res['_id'])
                    },
                    {
                        "$set": temp_op_data,
                    }
                )

                temp_op_data = {}
                op_data['PlanID'] = str(temp_res['_id'])
            else:
                # print('进入 insert')
                op_data['PlanNo'] = sync.mid('ProductPlan', 8, 'P')
                op_data['business_type_id'] = ObjectId(const_independent_travel_businessTypeID)
                op_data['business_type'] = const_independent_travel_businessType
                op_data['CompanyInfo'] = const_CompanyInfo
                op_data['ProductType'] = const_product_type
                op_data['SubTypeSource'] = 1
                op_data['Belong'] = product_data['Belong']

                sync.insert_plan(op_data)

                return {
                    'type': 'insert',
                    'document': op_data,
                }

                op_data['PlanID'] = str(insert_plan_res['_id'])

            sync.create_plan_price(op_data, price, sku_info, detail['item_id'])

            return True
        except BaseException as err:
            log = {
                'Function': 'create_plan',
                'Subtype': 49,
                'Message': 'Exception create_plan: ' + str(err),
            }
            sync.add_log(log)
            # 记录日志
            return False

    # 删除无关价格方案
    def del_unrelated_planprice(self, PlanID='', item_id=''):
        temp_condition = {
            'fliggy_item_id': item_id,
            'syncNo': {'$ne': sync_no},
        }

        if PlanID:
            temp_condition['PlanID'] = PlanID

        col_ProductPlanPrice.update_many(
            temp_condition,
            {
                "$set": {
                    "IsDel": 1,
                    "DelTime": now_iso_date,
                    "DelSource": 'DelUnrelatedPlanPrice',
                }
            },
        )

    # 更新单个计划余位(库存)
    def update_plan_stock(self, PlanID):
        temp_condition = {
            'IsDel': 0,
            'PlanID': ObjectId(PlanID),
        }

        temp_project = {
            'fliggy_stock': 1,
        }

        temp_res = col_ProductPlanPrice.find(temp_condition, temp_project)

        sum_stock = 0
        for one in temp_res:
            # sum_stock = sum_stock + int(one['AliField']['stock'])
            sum_stock = sum_stock + int(one['fliggy_stock'])

        col_ProductPlan.update_one(
            {
                '_id': ObjectId(PlanID)
            },
            {
                "$set": {
                    'PlanNumber': sum_stock,
                    'MorethanNumber': sum_stock,
                },
            }
        )

    def create_plan_price_bulk(self, plan_data, item_id, pre_plan_price_list):
        """
        创建价格方案
        :param      package_desc        套餐描述
        """
        try:
            op_data = {}
            op_data['AddInfo'] = const_AddInfo
            op_data['ProductType'] = const_product_type
            op_data['SubTypeSource'] = 1
            op_data['PlanID'] = ObjectId(plan_data['_id'])
            op_data['ProductID'] = ObjectId(plan_data['ProductID'])
            op_data['PlanNumber'] = plan_data['PlanNumber']
            op_data['MorethanNumber'] = plan_data['MorethanNumber']
            op_data['SingleRoomDiff'] = 0.0
            op_data['DefaultPrice'] = 0.0
            op_data['ChildPrice'] = 0.0
            op_data['BabyPrice'] = 0.0
            op_data['FliggyPriceType'] = int(plan_data['fliggy_price_type'])
            op_data['fliggy_item_id'] = str(item_id)
            op_data['fliggy_outer_sku_id'] = str(plan_data['fliggy_outer_sku_id'])
            op_data['fliggy_date'] = str(plan_data['fliggy_date'])
            op_data['fliggy_price_type'] = str(plan_data['fliggy_price_type'])
            op_data['fliggy_stock'] = str(plan_data['fliggy_stock'])
            op_data['fliggy_price'] = str(plan_data['fliggy_price'])
            op_data['syncNo'] = sync_no

            price_type = ''
            try:
                if int(plan_data['fliggy_price_type']) == 1:
                    price_type = '成人'
                    op_data['DefaultPrice'] = float(int(plan_data['fliggy_price']) / 100)

                if int(plan_data['fliggy_price_type']) == 2:
                    price_type = '小孩'
                    op_data['ChildPrice'] = float(int(plan_data['fliggy_price']) / 100)

                if int(plan_data['fliggy_price_type']) == 3:
                    price_type = '单房差'
                    op_data['SingleRoomDiff'] = float(int(plan_data['fliggy_price']) / 100)
            except BaseException as err:
                pass

            op_data['Pricetitle'] = str(plan_data['fliggy_package_name'] + '-' + str(price_type))

            if 'fliggy_package_desc' in plan_data:
                op_data['Remark'] = str(plan_data['fliggy_package_desc'])
            else:
                op_data['Remark'] = None

            index = str(plan_data['fliggy_outer_sku_id']) + str(plan_data['fliggy_date'])

            if index in pre_plan_price_list:
                op_data['UpdateTime'] = now_iso_date
                price_data = pre_plan_price_list[index]
                op_data.pop('PlanID')
                op_data.pop('ProductID')

                return UpdateOne(
                    {
                        "_id": ObjectId(price_data['_id'])
                    },
                    {
                        "$set": op_data
                    },
                )
            else:
                return sync.insert_plan_price(op_data)

        except BaseException as err:
            if const_need_print:
                print('Exception create_plan_price_bulk: ', err)

            return False

    # 创建价格方案
    # 停用
    def create_plan_price(self, param, price, sku_info, item_id):
        """
        创建价格方案
        :param      package_desc        套餐描述
        """
        try:
            temp_condition = {
                'IsDel': 0,
                'PlanID': ObjectId(param['PlanID']),
                'AliField.date': str(price['date']),
                'AliField.price_type': str(price['price_type']),
            }

            temp_project = {
                '_id': 1,
            }

            temp_res = col_ProductPlanPrice.find_one(temp_condition, temp_project)

            op_data = {}
            op_data['AddInfo'] = const_AddInfo
            op_data['ProductType'] = const_product_type
            op_data['SubTypeSource'] = 1
            op_data['PlanID'] = ObjectId(param['PlanID'])
            op_data['ProductID'] = ObjectId(param['ProductID'])
            op_data['PlanNumber'] = param['PlanNumber']
            op_data['MorethanNumber'] = param['MorethanNumber']
            op_data['SingleRoomDiff'] = 0.0
            op_data['DefaultPrice'] = 0.0
            op_data['ChildPrice'] = 0.0
            op_data['BabyPrice'] = 0.0
            op_data['FliggyPriceType'] = int(price['price_type'])
            op_data['AliField'] = {
                'item_id': str(item_id),
                'outer_sku_id': str(sku_info['outer_sku_id']),
                'date': str(price['date']),
                'price_type': str(price['price_type']),
                'stock': str(price['stock']),
                'price': str(price['price']),
            }

            price_type = ''
            try:
                if int(price['price_type']) == 1:
                    price_type = '成人'
                    op_data['DefaultPrice'] = float(price['price'] / 100)

                if int(price['price_type']) == 2:
                    price_type = '小孩'
                    op_data['ChildPrice'] = float(price['price'] / 100)

                if int(price['price_type']) == 3:
                    price_type = '单房差'
                    op_data['SingleRoomDiff'] = float(price['price'] / 100)
            except BaseException as err:
                pass

            op_data['Pricetitle'] = str(sku_info['package_name'] + '-' + str(price_type))

            if 'package_desc' in sku_info:
                op_data['Remark'] = str(sku_info['package_desc'])
            else:
                op_data['Remark'] = None

            if temp_res:
                op_data.pop('PlanID')
                op_data.pop('ProductID')

                col_ProductPlanPrice.update_one(
                    {
                        '_id': ObjectId(temp_res['_id'])
                    },
                    {
                        "$set": op_data,
                    }
                )
            else:
                sync.insert_plan_price(op_data)
        except BaseException as err:
            if const_need_print:
                print('Exception create_plan_price: ', err)
            # 记录日志

    # 插入价格方案
    def insert_plan_price(self, op_data):
        op_data['ID'] = 0
        op_data['AddTime'] = now_iso_date
        op_data['IsDel'] = 0
        op_data['isDefault'] = 0
        op_data['RetainNumber'] = 0
        op_data['PersistenceNumber'] = 0
        op_data['PaidNumber'] = 0
        op_data['IsDeductiblevoucher'] = 0
        op_data['Deductiblevoucher'] = 0
        op_data['SpecialRemark'] = const_special_remark

        return InsertOne(op_data)

        result = col_ProductPlanPrice.insert_one(op_data)

    # 插入计划
    def insert_plan(self, op_data):
        op_data['AddTime'] = now_iso_date
        op_data['TaxTypeID'] = 2
        op_data['PlanLabels'] = []
        op_data['ID'] = 0
        op_data['Station'] = {
            'ForeignKeyID': ObjectId('000000000000000000000001'),
            'Station_Name': '宁波',
        }
        op_data['ReserveTime'] = 1
        op_data['ShowStatus'] = 0
        op_data['ExpectProfitAverage'] = 0
        op_data['Allaccompanytourguide'] = None
        op_data['Networkorder'] = 1
        op_data['PaidOrderNumber'] = 0
        op_data['PersistenceOrderNumber'] = 0
        op_data['Lowestformation'] = 1
        op_data['AdultMachineBuildingFee'] = 0.0
        op_data['ChildMachineBuildingFee'] = 0.0
        op_data['AdultShipPortFee'] = 0.0
        op_data['ChildShipPortFee'] = 0.0
        op_data['ExitVisaFee'] = 0.0
        op_data['Cars'] = []
        op_data['IsBackstageHide'] = 0
        op_data['PlanTheCost'] = None
        op_data['TourGuideServiceCharge'] = 0.0
        op_data['TrainStatus'] = 0
        op_data['ShareStatus'] = 0
        op_data['PrepayInfo'] = {}
        op_data['SupplierInfo'] = []
        op_data['RetainNumber'] = 0
        op_data['SpecialRemark'] = const_special_remark

        return op_data

        result = col_ProductPlan.insert_one(op_data)

        return {
            '_id': str(result.inserted_id),
        }

    # 封装新计划信息
    def set_plan_info(self, product_data, price, detail):
        date_str = str(datetime.datetime.strptime(price['date'], "%Y-%m-%d %H:%M:%S"))
        date_timestamp = int(time.mktime(time.strptime(date_str, "%Y-%m-%d %H:%M:%S")))
        end_timestamp = date_timestamp + 86400 * int(detail['base_info']['trip_max_days'])
        end_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_timestamp))

        try:
            PlanNumber = int(price['stock'])
        except BaseException as err:
            PlanNumber = 0

        return {
            'IsDel': 0,
            'ProductNo': str(product_data['ProductNo']),
            'ProductID': ObjectId(product_data['_id']),
            'ProductName': str(product_data['ProductName']),
            'ProductType': const_product_type,
            'SubTypeSource': 1,
            # 'StartTime': isoformat(datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")),
            # 'EndTime': isoformat(datetime.datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")),
            'StartTime': cst_tz.localize(parser.parse(date_str)),
            'EndTime': cst_tz.localize(parser.parse(end_str)),
            'ProductGADDR': product_data['ProductGADDR'],
            'TravelDays': int(detail['base_info']['trip_max_days']),
            'LateNight': int(detail['base_info']['accom_nights']),
            'PlanNumber': PlanNumber,
            'MorethanNumber': PlanNumber,
            'AddInfo': const_AddInfo,
            'Belong': product_data['Belong'],
        }

    # 创建产品
    def create_product(self, detail, sku_info, approve_status):
        temp_condition = {
            'IsDel': 0,
            'AliField.item_id': detail['item_id'],
            'AliField.sku_info.outer_sku_id': sku_info['outer_sku_id'],
        }

        temp_project = {
            'ProductNo': 1,
            'Title': 1,
            'ProductGADDR': 1,
            'AliField': 1,
            'business_type': 1,
            'business_type_id': 1,
            'AddInfo': 1,
            'CompanyInfo': 1,
            'ProductType': 1,
            'SubTypeSource': 1,
            'Belong': 1,
        }

        temp_res = col_ProductTeamtour.find_one(temp_condition, temp_project)

        op_data = sync.set_product_info(detail, sku_info, approve_status, temp_res)

        if temp_res:
            # 已经在erp生成过产品
            # op_data.pop('ProductNo')
            op_data['UpdateTime'] = now_iso_date

            col_ProductTeamtour.update_one(
                {
                    '_id': temp_res['_id']
                },
                {
                    "$set": op_data,
                }
            )

            # return {
            #     '_id': str(temp_res['_id']),
            #     'ProductNo': temp_res['ProductNo'],
            #     'ProductName': temp_res['Title'],
            #     'ProductGADDR': temp_res['ProductGADDR'],
            #     'AliField': temp_res['AliField'],
            #     'business_type': temp_res['business_type'],
            #     'business_type_id': temp_res['business_type_id'],
            #     'AddInfo': temp_res['AddInfo'],
            #     'CompanyInfo': temp_res['CompanyInfo'],
            #     'ProductType': temp_res['ProductType'],
            #     'SubTypeSource': 1,
            #     'Belong': temp_res['Belong'],
            # }
        else:
            # 尚未在erp生成过产品
            result = sync.insert_product(op_data)

            # return {
            #     '_id': str(result['_id']),
            #     'ProductNo': op_data['ProductNo'],
            #     'ProductName': op_data['Title'],
            #     'ProductGADDR': op_data['ProductGADDR'],
            #     'AliField': op_data['AliField'],
            #     'business_type': op_data['business_type'],
            #     'business_type_id': op_data['business_type_id'],
            #     'AddInfo': result['AddInfo'],
            #     'CompanyInfo': result['CompanyInfo'],
            #     'ProductType': result['ProductType'],
            #     'SubTypeSource': 1,
            #     'Belong': result['Belong'],
            # }

    # 插入产品
    def insert_product(self, op_data):
        op_data['AddInfo'] = const_AddInfo
        op_data['AddTime'] = now_iso_date
        op_data['CompanyInfo'] = const_CompanyInfo
        op_data['IsDel'] = 0
        op_data['ProductType'] = const_product_type
        op_data['SubTypeSource'] = 1
        op_data['StationID'] = ObjectId('000000000000000000000001')
        op_data['Sort'] = 65535
        op_data['ContractTypeID'] = 1
        op_data['business_type_id'] = ObjectId(const_independent_travel_businessTypeID)
        op_data['business_type'] = const_independent_travel_businessType
        op_data['EnglishTitle'] = ''
        op_data['SEOTitle'] = ''
        op_data['SEOContent'] = ''
        op_data['Insurances'] = []
        op_data['ShowStatus'] = 0
        op_data['TrainStatus'] = 0
        op_data['ShareStatus'] = 0
        op_data['MinPrice'] = 0
        # op_data['Belong'] = op_data['AddInfo']
        op_data['LineType'] = {
            'ID': 3,
            'Name': '出境旅游',
        }
        op_data['IsVisa'] = 0
        op_data['SpecialRemark'] = const_special_remark

        result = col_ProductTeamtour.insert_one(op_data)

        # return {
        #     '_id': str(result.inserted_id),
        #     'business_type_id': op_data['business_type_id'],
        #     'business_type': op_data['business_type'],
        #     'ProductType': op_data['ProductType'],
        #     'SubTypeSource': 1,
        #     'AddInfo': op_data['AddInfo'],
        #     'Belong': op_data['Belong'],
        #     'CompanyInfo': op_data['CompanyInfo'],
        # }

    # 封装 与阿里数据有业务逻辑关系的 产品数据
    def set_product_info(self, detail, sku_info, approve_status, temp_res):
        param = {}

        tempDetail = copy.deepcopy(detail)
        tempDetail.pop('sku_infos')
        tempSkuInfo = copy.deepcopy(sku_info)
        tempDetail['sku_info'] = tempSkuInfo
        param['AliField'] = tempDetail
        param['AliField']['sku_info'].pop('combos')

        param['ApproveStatus'] = str(approve_status)
        if not temp_res:
            param['ProductNo'] = sync.mid('ProductTeamtour', 7, 'SX')
        param['Title'] = str((detail['base_info']['title'] + ' ' + str(sku_info['package_name'])))
        param['WebTitle'] = param['Title']
        param['SubTitle'] = param['Title']
        param['TravelDays'] = int(detail['base_info']['trip_max_days'])
        param['LateNight'] = param['TravelDays'] - 1
        param['StartGADDR'] = sync.set_start_GADDR(sku_info)
        param['ProductGADDR'] = sync.set_product_GADDR(sku_info)
        param['Belong'] = sync.set_belong(param['ProductGADDR']['Title'])
        param['syncNo'] = sync_no

        # 途径地
        param['ViaLand'] = str(detail['base_info']['to_locations'])
        # 默认图片
        for pic_url in detail['base_info']['pic_urls']['string']:
            param['DefaultPic'] = str(pic_url)
            break
        # 去程交通
        try:
            param['GoTraffic'] = sync.set_traffic(int(detail['freedom_item_ext']['go_traffic_info']['traffic_type']))
        except BaseException as err:
            pass
        # 回程交通
        try:
            param['GoTraffic'] = sync.set_traffic(int(detail['freedom_item_ext']['go_traffic_info']['traffic_type']))
        except BaseException as err:
            pass

        # 产品亮点
        try:
            text = ''
            index = 0
            for sub_title in detail['base_info']['sub_titles']['string']:
                text = text + str((index + 1)) + '.' + str(sub_title) + '。'
                index += 1
            param['LineFeatures'] = text

        except BaseException as err:
            pass

        return param

    # 封装交通信息
    def set_traffic(self, traffic_type):
        if traffic_type == 1:
            return {
                'ForeignKeyID': ObjectId('000000000000000000000002'),
                'DelStatus': 0,
                'Name': '飞机',
            }

        if traffic_type == 2:
            return {
                'ForeignKeyID': ObjectId('000000000000000000000003'),
                'DelStatus': 0,
                'Name': '火车',
            }

        if traffic_type == 3:
            return {
                'ForeignKeyID': ObjectId('000000000000000000000001'),
                'DelStatus': 0,
                'Name': '汽车',
            }

        if traffic_type == 4:
            return {
                'ForeignKeyID': ObjectId('000000000000000000000005'),
                'DelStatus': 0,
                'Name': '邮轮',
            }

        return {}

    # 封装出发地
    def set_start_GADDR(self, sku_info):
        combos = sku_info['combos']
        combos = json.loads(combos)

        if 'comboName' not in combos:
            combos = combos[0]

        if 'c' not in combos['from']:
            from_add = combos['from'][0]
        else:
            from_add = combos['from']

        c = from_add['c']
        c = c.split(':')[1]

        try:
            return pre_BasicsGADDRData[str(c)]
        except BaseException as err:
            return pre_BasicsGADDRData['宁波']

        # temp_condition = {
        #     'DelStatus': 0,
        #     'Title': str(c),
        # }
        #
        # temp_res = col_BasicsGADDR.find_one(temp_condition)
        #
        # if not temp_res:
        #     temp_condition = {
        #         'DelStatus': 0,
        #         'Title': '宁波',
        #     }
        #
        #     temp_res = col_BasicsGADDR.find_one(temp_condition)
        #
        # return {
        #     'ForeignKeyID': ObjectId(temp_res['_id']),
        #     'DelStatus': 0,
        #     'Title': temp_res['Title'],
        #     'TitleEN': temp_res['TitleEN'],
        #     'ParentID': temp_res['parentid'],
        #     'ParentName': temp_res['ParentName'],
        #     'Shorter': temp_res['Shorter'],
        #     'OrderBy': 0,
        #     'Parents': temp_res['Parents'],
        # }

    # 封装目的地
    def set_product_GADDR(self, sku_info):
        combos = sku_info['combos']
        combos = json.loads(combos)

        if 'comboName' not in combos:
            combos = combos[0]

        if 'cy' not in combos['to']:
            to_add = combos['to'][0]
        else:
            to_add = combos['to']

        cy = to_add['cy']
        cy = cy.split(':')[1]

        try:
            return pre_BasicsGADDRData[str(cy)]
        except BaseException as err:
            return pre_BasicsGADDRData['日本']

        # temp_condition = {
        #     'DelStatus': 0,
        #     'Title': str(cy),
        # }
        #
        # temp_res = col_BasicsGADDR.find_one(temp_condition)
        #
        # if not temp_res:
        #     temp_condition = {
        #         'DelStatus': 0,
        #         'Title': '日本',
        #     }
        #
        #     temp_res = col_BasicsGADDR.find_one(temp_condition)
        #
        # return {
        #     'ForeignKeyID': ObjectId(temp_res['_id']),
        #     'DelStatus': 0,
        #     'Title': temp_res['Title'],
        #     'TitleEN': temp_res['TitleEN'],
        #     'ParentID': temp_res['parentid'],
        #     'ParentName': temp_res['ParentName'],
        #     'Shorter': temp_res['Shorter'],
        #     'OrderBy': 0,
        #     'Parents': temp_res['Parents'],
        # }

    def set_belong(self, ProductGADDR):
        all_address_belong_data = sync.get_orderclaimconfiguration()

        default_address = {}

        for address in all_address_belong_data:
            add = {
                'ForeignKeyID': address['OPUser']['ForeignKeyID'],
                'EmployeeName': address['OPUser']['EmployeeName'],
                'EmployeeDepartmentID': address['Department']['_id'],
                'EmployeeDepartmentName': address['Department']['Name'],
            }

            if str(address['Address']['ForeignKeyID'] == '000000000000000000000000'):
                default_address = add

            if ProductGADDR == address['Address']['AddressName']:
                return add

        return default_address

    # 获取目的地-私顾计调 对应数据
    def get_orderclaimconfiguration(self):
        condition = {
            'OrderType.ForeignKeyID': ObjectId('000000000000000000000037'),
            'DelStatus': 0,
            'OrderSourceTypeID': 100001,
        }

        lookup = {
            'from': "Personnel",
            "localField": "OPUser.ForeignKeyID",
            "foreignField": "_id",
            "as": "Personnel",
        }

        lookup2 = {
            'from': "Department",
            "localField": "Personnel.Department.ID",
            "foreignField": "_id",
            "as": "Department",
        }

        project = {
            'Address': 1,
            'OPUser': 1,
            'Department.Name': 1,
            'Department._id': 1,
        }

        unwind = {
            '$unwind': '$Department',
        }

        list = col_OrderClaimConfiguration.aggregate(
            [
                {'$match': condition},
                {'$lookup': lookup},
                {'$lookup': lookup2},
                {'$project': project},
                unwind,
            ]
        )

        return list

    def single_query(self, num_iid, times):
        try:
            # -*- coding: utf-8 -*-
            import top.api

            req = top.api.AlitripTravelItemSingleQueryRequest('gw.api.taobao.com', 80)
            req.set_app_info(top.appinfo(const.appkey_main, const.secret_main))

            req.item_id = num_iid
            try:
                resp = req.getResponse(const_sessionkey_main)
                return resp
            except BaseException as err:
                if const_need_print:
                    print(err)

            # 旧写法, 调用php的接口
            # url = 'http://172.16.61.24:8899/fliggy/Shixian/singleQuery?num_iid=' + str(num_iid)
            # r = requests.post(url)
            #
            # return r.json()

        except BaseException as err:
            if const_need_print:
                print('Exception single_query: ', err)

            if times < 10:
                times += 1

                time.sleep(1)
                if const_need_print:
                    print('第', times, '次重试single_query')
                sync.single_query(num_iid, times)

    def get_num_iid_list(self):
        try:
            result = sync.get_list_post()
            num_iid_list = []
            approve_status_list = {}

            if result['response']['status'] == 701:
                list = json.loads(result['response']['data'])

                if len(list) == 0:
                    pass
                    # 日志
                else:
                    outer_id_list = []

                    for data in list:
                        num_iid_list.append({
                            'num_iid': data['num_iid'],
                            'outer_id': data['outer_id'],
                        })
                        approve_status_list[str(data['num_iid'])] = data['approve_status']

                        if 'outer_id' in data:
                            if data['outer_id'] and data['outer_id'] != '1234':
                                outer_id = str(data['outer_id'])
                                if outer_id in outer_id_list:
                                    log = {
                                        'Function': 'main',
                                        'Subtype': 11,
                                        'Message': 'outer_id: ' + str(outer_id) + ' 存在相同, 请检查后重试',
                                    }
                                    sync.add_log(log)
                                    return False

                                outer_id_list.append(outer_id)

            if len(num_iid_list) == 0:
                pass
                # 日志
            else:
                return {
                    'num_iid_list': num_iid_list,
                    'approve_status_list': approve_status_list,
                }

        except BaseException as err:
            if const_need_print:
                print('Exception get_num_iid_list: ', err)

    def get_list_post(self):
        try:
            param = {}
            now_str = (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

            param['app_key'] = const_appkey_main
            param['method'] = 'iflying.shixian.list.get'
            param['v'] = '2.0'
            param['target_app_key'] = param['app_key']
            param['partner_id'] = partner_id
            param['timestamp'] = str(now_str)
            param['sign_method'] = 'md5'
            param['format'] = 'json'
            param[
                'RequestData'] = '{"orderType":2,"banner":"","cid":50278002,"seller_cids":"","orderBy":"","startModified":"","endModified":""}'

            sign = sync.sign(const_secret_main, param)
            url = main_base_url + '' + '?RequestData=' + urllib.parse.quote(param['RequestData']) + '&app_key=' + param[
                'app_key'] + '&method=' + param['method'] + '&v=' + param[
                      'v'] + '&sign=' + sign + '&timestamp=' + urllib.parse.quote(
                param['timestamp']) + '&target_app_key=' + param['target_app_key'] + '&partner_id=' + param[
                      'partner_id'] + '&format=' + param['format'] + '&sign_method=' + param['sign_method']
            r = requests.post(url)
            return r.json()

        except BaseException as err:
            if const_need_print:
                print('Exception get_list_post: ', err)

    def sign(self, secret, parameters):
        """
        生成签名 (taobao-sdk 中搬运的方法)
        逻辑文档: https://open.taobao.com/doc.htm?docId=101617&docType=1
        """
        # ===========================================================================
        # '''签名方法
        # @param secret: 签名需要的密钥
        # @param parameters: 支持字典和string两种
        # '''
        # ===========================================================================
        # 如果parameters 是字典类的话
        if hasattr(parameters, "items"):
            # 原sdk写法为python2, 两处经过调整
            # keys = parameters.keys()
            # keys.sort()
            keys = sorted(parameters)

            parameters = "%s%s%s" % (secret,
                                     str().join('%s%s' % (key, parameters[key]) for key in keys),
                                     secret)

        m5 = hashlib.md5()
        m5.update(parameters.encode('utf-8'))
        sign = m5.hexdigest().upper()
        return sign

    def init_fliggy_param(self):
        temp_condition = {
            'DelStatus': 0,
            '_id': {
                '$in': [
                    ObjectId('000000000000000000000001'),
                    ObjectId('000000000000000000000002'),
                ]
            },
        }

        res = col_AliFliggyStores.find(temp_condition)

        if not res:
            log = {
                'Function': 'init_fliggy_param',
                'Subtype': 1,
                'Message': '获取飞猪店铺数据失败',
                'Data': {
                    'Result': res,
                },
            }
            sync.add_log(log)
            exit()

        global const_sessionkey_main
        global const_appkey_main
        global const_secret_main

        for data in res:
            if str(data['_id']) == '000000000000000000000001':
                LC_SHOP = data
                continue

            if str(data['_id']) == '000000000000000000000002':
                MAIN_SHOP = data
                continue

        const_appkey_main = str(MAIN_SHOP['AppId'])
        const_secret_main = str(MAIN_SHOP['AppSecret'])
        const_sessionkey_main = str(MAIN_SHOP['Session'])

    def mid(self, collection, digit, condition):
        """
        生成唯一编号
        :param
            collection      表名
            digit           长度
            condition       前缀符号
        """

        res = col_ids.find_one_and_update(
            {
                'name': collection,
            },
            {
                '$inc': {
                    'id': 1
                },
            },
            projection={'id': True, '_id': False},
            return_document=ReturnDocument.AFTER,
        )

        NO = int(res['id'])

        s = '%0' + str(digit) + 'd'
        NO = s % NO

        return str(condition) + NO

    # 添加日志
    def add_log(self, data):
        msg = (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'), ': ', data['Message']
        if const_need_print:
            print(msg)

        dir = 'log'
        if const_need_print:
            if not os.path.exists(dir):
                os.makedirs(dir)

            log_file_name = dir + '/sync_fliggy_product_to_erp.txt'
            with open(log_file_name, 'a') as file_obj:
                file_obj.write(str(msg) + " \n")

        insert_data = {
            'userID': ObjectId('5d8c5c2228142658548b456a'),
            'userName': '系统管理员Online',
            'departmentID': ObjectId('000000000000000000000781'),
            'departmentName': '浙江恒越信息科技有限公司',
            'time': now_iso_date,
            'type': 26,
            'data': {
                'Language': 'python3',
                'Class': 'sync_fliggy_product_to_erp',
            },
        }

        insert_data['data'].update(data)

        col_ChangeLog.insert_one(insert_data)


class MyThread(threading.Thread):
    def __init__(self, func, args, name=''):
        threading.Thread.__init__(self)
        self.name = name
        self.func = func
        self.args = args
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None


try:
    start = time.perf_counter()

    sync = sync()

    global const_need_print

    const_need_print = True
    try:
        if int(sys.argv[1]) == 1:
            const_need_print = False
    except BaseException as err:
        pass

    sync.init_fliggy_param()

    list_res = sync.get_num_iid_list()

    if not list_res:
        log = {
            'Function': 'main',
            'Subtype': 2,
            'Message': '列表为空',
            'Result': list_res,
        }
        sync.add_log(log)
        exit()

    sync.main(1, {
        'num_iid': '598145720467',
        'outer_id': '598145720467',
    })

    # 方法一
    # i = 0
    # for goods in list_res['num_iid_list']:
    #     i += 1
    #
    #     try:
    #         _thread.start_new_thread(sync.main, ("Thread-" + str(i), goods))
    #     except:
    #         print("Error: 无法启动线程")
    #
    #     time.sleep(0.1)
    #
    # time.sleep(10)

    # 方法二
    # threads = []
    # i = 0
    # no = range(len(list_res['num_iid_list']))
    # for i in no:
    #     t = MyThread(sync.main, ("Thread-" + str(i), list_res['num_iid_list'][i]))
    #     threads.append(t)
    # for i in no:  # start threads 此处并不会执行线程，而是将任务分发到每个线程，同步线程。等同步完成后再开始执行start方法
    #     threads[i].start()
    #
    # for i in no:  # join()方法等待线程完成
    #     threads[i].join()

    if const_need_print:
        elapsed = (time.perf_counter() - start)
        print("Time used:", elapsed)
finally:
    print('final result:100')
