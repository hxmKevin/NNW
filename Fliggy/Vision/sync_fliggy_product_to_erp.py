# coding=UTF-8
# !/usr/bin/python3

# Title : 定期同步飞猪产品至erp
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
import hashlib
import _md5
import top.api
import _thread
import threading

global no_outer_id_product_list
global not_modified_price_id_list
no_outer_id_product_list = []
not_modified_price_id_list = []

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

# lc_base_url = 'http://80yrm8bm30.api.taobao.com/router/qmtest'
# main_base_url = 'http://11186295n9.api.taobao.com/router/qmtest'
lc_base_url = 'http://80yrm8bm30.api.taobao.com/router/qm'
main_base_url = 'http://11186295n9.api.taobao.com/router/qm'
partner_id = 'taobao-sdk-python-20190912'

const_appkey_main = '27761322'
const_secret_main = '98e36ac9fa56d9c5af04a6777a67d7af'
const_sessionkey_main = ''
score_obj_saler = '58d9c05f22393b87b081b23c'
toggle_mq_queue_url = '/Queue/QueueManage/ResetControlQueueStatus'

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

if __name__ == "__main__":
    mongodb = mongodb_config.get_mongodb_config()
    mongodb_ip = mongodb['mongodb_ip']
    mongodb_port = mongodb['mongodb_port']
    mongodb_auth = mongodb['mongodb_auth']
    mongodb_password = mongodb['mongodb_password']
    rpc_url = str(mongodb['rpc_url'])

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
col_ChangeLog = db.FliggyLog
col_ids = db.ids
col_PlatformProductNumberConfig = db.PlatformProductNumberConfig


# newest SubType 20
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

            if const_need_print:
                print('item_id: ', num_iid, 'main 处理开始')

            outer_id_exists = True
            if 'outer_id' not in goods:
                outer_id_exists = False

            if not goods['outer_id']:
                outer_id_exists = False

            if not outer_id_exists:
                log = {
                    'Function': 'main',
                    'Subtype': 9,
                    'Message': 'item_id: ' + str(num_iid) + ' outer_id 字段不存在, 跳过处理',
                    'Data': goods,
                }
                sync.add_log(log)
                return False

            temp_res = sync.single_query(num_iid, 0)

            log = {
                'Function': 'main',
                'Subtype': 19,
                'Message': str(num_iid) + '添加详情日志',
                'Data': temp_res,
            }
            sync.add_log(log)

            if not temp_res:
                # 记录日志
                temp_msg = 'main: item_id: ' + str(num_iid) + ' 详情查询失败, 请检查或联系管理员'
                if const_need_print:
                    print(temp_msg)

                log = {
                    'Function': 'main',
                    'Subtype': 18,
                    'Message': temp_msg,
                    'Result': temp_res,
                }
                sync.add_log(log)
                final_message.append(log['Message'])
                return False

            if 'alitrip_travel_item_single_query_response' not in temp_res:
                # 记录日志
                if const_need_print:
                    print('main: item_id: ' + str(num_iid) + ' alitrip_travel_item_single_query_response 字段不存在')

                log = {
                    'Function': 'main',
                    'Subtype': 2,
                    'Message': 'main: item_id: ' + str(num_iid) + ' alitrip_travel_item_single_query_response 字段不存在',
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
                    'Message': 'main: item_id: ' + str(num_iid) + ' travel_item 字段不存在',
                    'Result': temp_res,
                }
                sync.add_log(log)
                return False

            if not temp_res['travel_item']:
                # 记录日志
                log = {
                    'Function': 'main',
                    'Subtype': 4,
                    'Message': 'main: item_id: ' + str(num_iid) + ' travel_item 字段为空',
                    'Result': temp_res,
                }
                sync.add_log(log)
                return False

            if 'out_id' not in temp_res['travel_item']['base_info']:
                # 记录日志
                log = {
                    'Function': 'main',
                    'Subtype': 5,
                    'Message': 'main: item_id: ' + str(num_iid) + ' out_id(商家编码) 字段不存在, 跳过处理',
                    'Result': temp_res,
                }
                sync.add_log(log)
                final_message.append(log['Message'])
                return False

            if not temp_res['travel_item']['base_info']['out_id']:
                # 记录日志
                log = {
                    'Function': 'main',
                    'Subtype': 6,
                    'Message': 'main: item_id: ' + str(num_iid) + ' out_id(商家编码) 字段为空, 跳过处理',
                    'Result': temp_res,
                }
                sync.add_log(log)
                final_message.append(log['Message'])
                return False

            detail = temp_res['travel_item']
            detail['base_info']['out_id'] = str(detail['base_info']['out_id'])
            detail['item_id'] = str(detail['item_id'])
            del temp_res

            if ignore_modified_time:
                pass
            elif 'modified' in detail and str(num_iid) in modified_product_list:
                if str(detail['modified']) == modified_product_list[str(num_iid)]:
                    log = {
                        'Function': 'main',
                        'Subtype': 14,
                        'Message': 'item_id: ' + str(num_iid) + ' 没有修改, 无需同步',
                        'Data': detail['base_info'],
                    }
                    sync.add_log(log)
                    final_message.append(log['Message'])
                    return False

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

            try:
                detail['base_info'].pop('wap_desc')
            except BaseException as err:
                pass

            """"""""""""""""""
            """ 有效操作开始 """
            """"""""""""""""""

            # vision_no = sync.set_vision_no(detail['base_info']['out_id'])

            try:
                # 有时候是pontus_travel_item_sku_info 有时候是 item_sku_info
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

                if not sku_infos:
                    return False

                # 检查outer_sku_id 是否存在重复
                if not sync.check_outer_sku_id_repeat(detail, sku_infos):
                    return False

                # 预先获取出发地/目的地数据
                pre_BasicsGADDRData = sync.get_pre_BasicsGADDRData(sku_infos)

                if 'package_name' in sku_infos:
                    # 存在sku信息不是list 格式的情况
                    sku_infos = [
                        sku_infos
                    ]

                outer_sku_id_list = []
                for sku_info in sku_infos:
                    if 'outer_sku_id' in sku_info:
                        if sku_info['outer_sku_id']:
                            outer_sku_id_list.append(str(sku_info['outer_sku_id']))
                    sync.do_create_product(detail, sku_info, list_res['approve_status_list'][str(num_iid)],
                                           pre_BasicsGADDRData)

                del sku_info
                del pre_BasicsGADDRData
                # 更新 PlatformProductNumberConfig 表
                if outer_sku_id_list:
                    sync.update_platform_product_number_config(num_iid, outer_sku_id_list, detail['base_info']['title'])

                temp_condition = {
                    'IsDel': 0,
                    # 'FliggyResetTime': {'$exists': False},
                    'fliggy_item_id': str(num_iid),
                }
                temp_project = {
                    'ProductNo': 1,
                    'ProductID': 1,
                    'PlanNumber': 1,
                    'MorethanNumber': 1,
                    # 'PersistenceNumber': 1,
                    'PaidNumber': 1,
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

                temp_condition = {
                    'IsDel': 0,
                    'AliField.item_id': str(num_iid),
                    'syncNo': sync_no,
                }
                temp_project = {
                    'ProductType': 1,
                    'ProductNo': 1,
                    'Title': 1,
                    'WebTitle': 1,
                    'ProductGADDR': 1,
                    'AliField': 1,
                    'Belong': 1,
                }
                operated_product_list = col_ProductTeamtour.find(temp_condition, temp_project)

                pre_plan_list = {}
                for plan_one in plan_list:
                    index = str(plan_one['fliggy_item_id']) + str(plan_one['fliggy_outer_sku_id']) + str(plan_one['fliggy_date'])
                    pre_plan_list[index] = plan_one

                del plan_list

                ns = []
                for product_data in operated_product_list:
                    product_data['ProductName'] = product_data.pop('Title')
                    sku_info = product_data['AliField']['sku_info']

                    ns = sync.do_create_plan(sku_info, detail, product_data, pre_plan_list, ns)

                if ns:
                    if len(ns) > 0:
                        col_ProductPlan.bulk_write(ns)
                        del ns

                del operated_product_list

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

            # 更新计划的余位, 主要用于 计划+价格方案 被清零后余位的校准
            sync.update_plan_number(detail['item_id'])

            # sync.handle_has_created_order_plan()

            log = {
                'Function': 'main',
                'Subtype': 7,
                'Message': 'item_id: ' + str(num_iid) + ' 处理成功',
                'Data': detail['base_info'],
            }
            final_message.append(log['Message'])
            sync.add_log(log)
        except BaseException as err:
            log = {
                'Function': 'main',
                'Subtype': 8,
                'Message': '请联系管理员检查, Exception main: item_id: ' + str(num_iid) + ': ' + str(err),
            }
            final_message.append(log['Message'])
            sync.add_log(log)
        finally:
            return 'success'

    def update_plan_number(self, item_id):
        """
        更新计划的余位, 主要用于 计划+价格方案 被清零后余位的校准
        """
        try:
            temp_condition = {
                'IsDel': 0,
                'FliggyResetTime': {'$exists': True},
                'fliggy_item_id': str(item_id),
            }
            temp_project = {
                'PlanNumber': 1,
                'MorethanNumber': 1,
                'PaidNumber': 1,
                'PersistenceNumber': 1,
                'RetainNumber': 1,
            }
            plan_list = col_ProductPlan.find(temp_condition, temp_project)

            updates_plan = []
            updates_price = []
            for plan in plan_list:
                set = {}
                try:
                    MorethanNumber = int(plan['MorethanNumber'])
                except BaseException as err:
                    set['MorethanNumber'] = 0
                    MorethanNumber = 0

                try:
                    PaidNumber = int(plan['PaidNumber'])
                except BaseException as err:
                    set['PaidNumber'] = 0
                    PaidNumber = 0

                try:
                    PersistenceNumber = int(plan['PersistenceNumber'])
                except BaseException as err:
                    set['PersistenceNumber'] = 0
                    PersistenceNumber = 0

                try:
                    RetainNumber = int(plan['RetainNumber'])
                except BaseException as err:
                    set['RetainNumber'] = 0
                    RetainNumber = 0

                set['PlanNumber'] = MorethanNumber + PaidNumber + PersistenceNumber + RetainNumber

                one = UpdateOne(
                    {
                        '_id': plan['_id'],
                    },
                    {
                        '$set': set
                    },
                )
                one_price = UpdateOne(
                    {
                        'PlanID': plan['_id'],
                    },
                    {
                        '$set': set
                    },
                )
                updates_plan.append(one)
                updates_price.append(one_price)

            if updates_plan:
                col_ProductPlan.bulk_write(updates_plan)

            if updates_price:
                col_ProductPlanPrice.bulk_write(updates_price)

        except BaseException as err:
            log = {
                'Function': 'update_plan_number',
                'Subtype': 49,
                'Message': 'Exception update_plan_number: ' + str(err),
            }
            sync.add_log(log)
            return


    def handle_has_created_order_plan(self):
        # 2019.11.13 未启用
        final_list = []

        for one in pre_plan_list:
            PersistenceNumber = int(pre_plan_list[one]['PersistenceNumber'])
            PaidNumber = int(pre_plan_list[one]['PaidNumber'])
            MorethanNumber = int(pre_plan_list[one]['MorethanNumber'])
            PlanNumber = int(pre_plan_list[one]['PlanNumber'])
            fliggy_stock = int(pre_plan_list[one]['fliggy_stock'])

            if (PaidNumber + PersistenceNumber) > 0 and MorethanNumber > 0 and PlanNumber > 0 and fliggy_stock > 0:
                final_list.append(ObjectId(pre_plan_list[one]['_id']))

        temp_condition = {
            '_id': {
                "$in": final_list
            },
        }

        col_ProductPlan.update_many(
            temp_condition,
            {
                "$set": {
                    "MorethanNumber": 0,
                    # "PlanNumber": 0,
                    "fliggy_stock": '0',
                    "FliggyResetTime": now_iso_date,
                }
            },
        )

    # 批量创建价格方案
    def create_plan_price_batch(self, num_iid):
        time.sleep(0.5)

        temp_condition = {
            'IsDel': 0,
            # 'FliggyResetTime': {'$exists': False},
            'fliggy_item_id': str(num_iid),
        }
        temp_project = {
            'PlanID': 1,
            'ProductID': 1,
            'PlanNumber': 1,
            'MorethanNumber': 1,
            'Pricetitle': 1,
            'fliggy_item_id': 1,
            'fliggy_date': 1,
            'fliggy_outer_sku_id': 1,
            'fliggy_stock': 1,
            'fliggy_price': 1,
        }
        res = col_ProductPlanPrice.find(temp_condition, temp_project)

        pre_plan_price_list = {}
        temp_condition = {
            'IsDel': 0,
            'FliggyResetTime': {'$exists': False},
            'fliggy_item_id': str(num_iid),
            'syncNo': sync_no,
        }
        temp_project = {
            'ProductID': 1,
            'ProductNo': 1,
            'PlanNo': 1,
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

        for price in res:
            index = str(price['fliggy_item_id']) + str(price['fliggy_outer_sku_id']) + str(price['fliggy_date'])
            pre_plan_price_list[index] = price

        ns = []
        for plan_data in plan_list:
            # if not ignore_modified_time:
            #     temp_index = str(plan_data['fliggy_item_id']) + str(plan_data['fliggy_outer_sku_id']) + str(plan_data['fliggy_date'])
            #     if temp_index in pre_plan_price_list:
            #         temp_res = sync.judge_price_modified(pre_plan_price_list[temp_index], plan_data)
            #         if not temp_res:
            #             not_modified_price_id_list.append(pre_plan_price_list[temp_index]['_id'])
            #             continue

            temp_res = sync.create_plan_price_bulk(plan_data, num_iid, pre_plan_price_list)

            if temp_res:
                ns.append(temp_res)
        if ns:
            col_ProductPlanPrice.bulk_write(ns)

    def judge_price_modified(self, price_data, plan_data):
        """
        判断价格方案是否发生改变
        @:return
            True    有改变
            False   没改变
        """
        if str(price_data['PlanID']) != str(plan_data['_id']):
            return 1

        if str(price_data['ProductID']) != str(plan_data['ProductID']):
            return 2

        if str(price_data['PlanNumber']) != str(plan_data['PlanNumber']):
            return 3

        if str(price_data['MorethanNumber']) != str(plan_data['MorethanNumber']):
            return 4

        if str(price_data['fliggy_stock']) != str(plan_data['fliggy_stock']):
            return 5

        if str(price_data['fliggy_price']) != str(plan_data['fliggy_price']):
            return 6

        if str(price_data['Pricetitle']) != (str(plan_data['fliggy_package_name'])+'-成人'):
            return 7

        return False

    def get_pre_BasicsGADDRData(self, sku_infos):
        """
        预先获取出发地/目的地数据
        """
        pre_BasicsGADDRData = {}

        title_list = ['宁波', '日本']

        for sku_info in sku_infos:
            combos = sku_info['combos']
            combos = json.loads(combos)

            c = '上海'
            cy = '日本'

            if 'comboName' not in combos:
                combos = combos[0]

            try:
                if 'c' not in combos['from']:
                    from_add = combos['from'][0]
                else:
                    from_add = combos['from']

                c = from_add['c']
                c = c.split(':')[1]
            except BaseException as err:
                pass

            try:
                if 'cy' not in combos['to']:
                    to_add = combos['to'][0]
                else:
                    to_add = combos['to']

                cy = to_add['cy']
                cy = cy.split(':')[1]
            except BaseException as err:
                pass

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

        return pre_BasicsGADDRData

    # 检查outer_sku_id 是否存在重复
    def check_outer_sku_id_repeat(self, detail, sku_infos):
        temp_list = []
        for sku_info in sku_infos:
            try:
                outer_sku_id = str(sku_info['outer_sku_id'])

                if outer_sku_id in temp_list:
                    log = {
                        'Function': 'check_outer_sku_id_repeat',
                        'Subtype': 10,
                        'Message': 'item_id: ' + str(detail['item_id']) + ' ' + str(outer_sku_id) + ' 存在相同套餐编码, 不进行同步',
                    }
                    sync.add_log(log)
                    final_message.append(log['Message'])
                    return False

                if not outer_sku_id:
                    log = {
                        'Function': 'check_outer_sku_id_repeat',
                        'Subtype': 16,
                        'Message': 'item_id: ' + str(detail['item_id']) + ' ' + str(outer_sku_id) + ' 存在空套餐编码, 不进行同步',
                    }
                    sync.add_log(log)
                    final_message.append(log['Message'])
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
                continue

            for price in prices:
                # print(price['date'])
                if int(price['price_type']) == 3:
                    one = UpdateOne(
                        {
                            'IsDel': 0,
                            'FliggyResetTime': {'$exists': False},
                            'fliggy_item_id': str(item_id),
                            'fliggy_outer_sku_id': str(outer_sku_id),
                            'fliggy_date': str(price['date']),
                        },
                        {
                            '$set': {
                                'SingleRoomDiff': float(int(price['price']) / 100),
                                'ShixianUpdateTime': now_iso_date,
                            }
                        },
                    )
                    updates.append(one)

        if updates:
            col_ProductPlanPrice.bulk_write(updates)

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
    def do_create_product(self, detail, sku_info, approve_status, pre_BasicsGADDRData):
        try:
            if 'outer_sku_id' not in sku_info:
                return

            if not sku_info['outer_sku_id']:
                # 记录日志
                return

            sku_info['outer_sku_id'] = str(sku_info['outer_sku_id'])
            create_product_res = sync.create_product(detail, sku_info, approve_status, pre_BasicsGADDRData)
        except BaseException as err:
            if const_need_print:
                print('Exception do_create_product: ', err)


    # 删除无关联的计划
    def del_unrelated_plan(self, product_data='', item_id=''):
        temp_condition = {
            'fliggy_item_id': str(item_id),
            'syncNo': {'$ne': sync_no},
            'FliggyResetTime': {'$exists': False},
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
                    # "IsDel": 1,
                    # "DelTime": now_iso_date,
                    # "DelSource": 'DelUnrelatedPlan',
                    "FliggyResetTime": now_iso_date,
                    # 'PlanNumber': 0,
                    'MorethanNumber': 0,
                    'fliggy_stock': '0',
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
            'FliggyResetTime': {'$exists': False},
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

    def do_create_plan(self, sku_info, detail, product_data, pre_plan_list, ns):
        try:
            prices = {}
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

            if not prices:
                log = {
                    'Function': 'do_create_plan',
                    'Subtype': 49,
                    'Message': str(detail['item_id']) + ': prices 为空',
                    'Data': detail,
                }
                sync.add_log(log)
                return ns

            if 'date' in prices:
                prices = [
                    prices
                ]

            i = 0
            for price in prices:
                i += 1

                if 'date' not in price:
                    continue

                res = sync.create_plan(sku_info, detail, product_data, price, pre_plan_list)

                if res != False:
                    if res['type'] == 'update':
                        final = UpdateOne(
                            res['filter'],
                            res['update'],
                        )
                        ns.append(final)

                    if res['type'] == 'insert':
                        final = InsertOne(res['document'])

                        ns.append(final)

            return ns
            # if len(ns) > 0:
            #     col_ProductPlan.bulk_write(ns)
        except BaseException as err:
            log = {
                'Function': 'do_create_plan',
                'Subtype': 49,
                'Message': 'Exception do_create_plan: ' + str(detail['item_id']) + ': ' + str(err),
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
            if 'package_desc' in sku_info:
                op_data['fliggy_package_desc'] = str(sku_info['package_desc'])
            op_data['fliggy_date'] = str(price['date'])
            op_data['fliggy_price'] = str(price['price'])
            op_data['fliggy_price_type'] = str(price['price_type'])
            op_data['fliggy_stock'] = str(price['stock'])

            index = str(detail['item_id']) + str(sku_info['outer_sku_id']) + str(price['date'])

            if index in pre_plan_list:
                id = ObjectId(pre_plan_list[index]['_id'])
                # erp中已生成计划
                temp_op_data = copy.deepcopy(op_data)

                temp_op_data['ShixianUpdateTime'] = now_iso_date
                # temp_op_data.pop('ProductNo')
                # temp_op_data.pop('ProductID')

                if 'fyself' in op_data['fliggy_outer_sku_id']:
                    try:
                        temp_op_data['PlanNumber'] = temp_op_data['MorethanNumber'] + pre_plan_list[index]['PaidNumber']
                    except BaseException as err:
                        print(str(detail['item_id']) + 'fyself 平衡余位异常')
                else:
                    temp_op_data.pop('PlanNumber')
                    temp_op_data.pop('MorethanNumber')

                # if pre_plan_list[index]['PlanNumber'] == 0 and pre_plan_list[index]['MorethanNumber'] == 0 and temp_op_data['PlanNumber'] > 0:
                #     pass
                # else:
                #     temp_op_data.pop('PlanNumber')
                #     temp_op_data.pop('MorethanNumber')
                #
                # pre_plan_list.pop(index)

                return {
                    'type': 'update',
                    'filter': {
                        "_id": id
                    },
                    'update': {
                        "$set": temp_op_data,
                        "$unset": {"FliggyResetTime": 1}
                    },
                }
            else:
                op_data['IsDel'] = 0
                op_data['ProductType'] = const_product_type
                op_data['PlanNo'] = sync.mid('ProductPlan', 8, 'P')
                op_data['business_type_id'] = ObjectId(const_independent_travel_businessTypeID)
                op_data['business_type'] = const_independent_travel_businessType
                op_data['CompanyInfo'] = const_CompanyInfo
                op_data['SubTypeSource'] = 1
                op_data['AddInfo'] = const_AddInfo

                sync.insert_plan(op_data)

                return {
                    'type': 'insert',
                    'document': op_data,
                }
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
            'fliggy_item_id': str(item_id),
            'syncNo': {'$ne': sync_no},
            'FliggyResetTime': {'$exists': False},
        }

        # if not_modified_price_id_list:
        #     temp_condition['_id'] = {
        #         "$nin": not_modified_price_id_list
        #     }

        if PlanID:
            temp_condition['PlanID'] = PlanID

        col_ProductPlanPrice.update_many(
            temp_condition,
            {
                "$set": {
                    # "IsDel": 1,
                    # "DelTime": now_iso_date,
                    # "DelSource": 'DelUnrelatedPlanPrice',
                    "FliggyResetTime": now_iso_date,
                    # 'PlanNumber': 0,
                    'MorethanNumber': 0,
                    'fliggy_stock': '0',
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
            op_data['ProductID'] = ObjectId(plan_data['ProductID'])
            op_data['ProductNo'] = plan_data['ProductNo']
            op_data['PlanID'] = ObjectId(plan_data['_id'])
            op_data['PlanNo'] = plan_data['PlanNo']
            op_data['PlanNumber'] = plan_data['PlanNumber']
            op_data['MorethanNumber'] = plan_data['MorethanNumber']
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

            index = str(plan_data['fliggy_item_id']) + str(plan_data['fliggy_outer_sku_id']) + str(plan_data['fliggy_date'])

            if index in pre_plan_price_list:
                op_data['ShixianUpdateTime'] = now_iso_date
                price_data = pre_plan_price_list[index]
                # op_data.pop('PlanID')
                # op_data.pop('ProductID')

                return UpdateOne(
                    {
                        "_id": ObjectId(price_data['_id'])
                    },
                    {
                        "$set": op_data,
                        "$unset": {"FliggyResetTime": 1}
                    },
                )
            else:
                op_data['AddInfo'] = const_AddInfo
                op_data['ProductType'] = const_product_type
                op_data['SubTypeSource'] = 1
                op_data['SingleRoomDiff'] = 0.0
                # op_data['DefaultPrice'] = 0.0
                op_data['ChildPrice'] = 0.0
                op_data['BabyPrice'] = 0.0

                return sync.insert_plan_price(op_data)

        except BaseException as err:
            if const_need_print:
                print('Exception create_plan_price_bulk: ', err)

            return False

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
        op_data['PersistenceNumber'] = 0
        op_data['PaidNumber'] = 0

        return op_data

    # 封装新计划信息
    def set_plan_info(self, product_data, price, detail):
        date_str = str(datetime.datetime.strptime(price['date'], "%Y-%m-%d %H:%M:%S"))
        date_timestamp = int(time.mktime(time.strptime(date_str, "%Y-%m-%d %H:%M:%S")))
        try:
            end_timestamp = date_timestamp + 86400 * int(detail['base_info']['trip_max_days'])
        except BaseException as err:
            end_timestamp = date_timestamp + 86400
        end_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_timestamp))

        try:
            PlanNumber = int(price['stock'])
        except BaseException as err:
            PlanNumber = 0

        return {
            'ProductNo': str(product_data['ProductNo']),
            'ProductID': ObjectId(product_data['_id']),
            'ProductName': str(product_data['ProductName']),
            'FliggyProductTitle': str(product_data['WebTitle']),
            # 'StartTime': isoformat(datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")),
            # 'EndTime': isoformat(datetime.datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S")),
            'StartTime': cst_tz.localize(parser.parse(date_str)),
            'EndTime': cst_tz.localize(parser.parse(end_str)),
            'ProductGADDR': product_data['ProductGADDR'],
            'TravelDays': int(detail['base_info']['trip_max_days']),
            'LateNight': int(detail['base_info']['accom_nights']),
            'PlanNumber': PlanNumber,
            'MorethanNumber': PlanNumber,
            'Belong': product_data['Belong'],
        }

    # 创建产品
    def create_product(self, detail, sku_info, approve_status, pre_BasicsGADDRData):
        temp_condition = {
            'IsDel': 0,
            'AliField.item_id': str(detail['item_id']),
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

        op_data = sync.set_product_info(detail, sku_info, approve_status, temp_res, pre_BasicsGADDRData)

        if temp_res:
            # 已经在erp生成过产品
            # op_data.pop('ProductNo')
            op_data['ShixianUpdateTime'] = now_iso_date

            col_ProductTeamtour.update_one(
                {
                    '_id': temp_res['_id']
                },
                {
                    "$set": op_data,
                }
            )
        else:
            # 尚未在erp生成过产品
            result = sync.insert_product(op_data)


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

        result = col_ProductTeamtour.insert_one(op_data)

    # 封装 与阿里数据有业务逻辑关系的 产品数据
    def set_product_info(self, detail, sku_info, approve_status, temp_res, pre_BasicsGADDRData):
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
        param['Title'] = str(sku_info['package_name'])
        param['WebTitle'] = str(detail['base_info']['title'])
        param['SubTitle'] = param['Title']
        param['TravelDays'] = int(detail['base_info']['trip_max_days'])
        param['LateNight'] = param['TravelDays'] - 1
        param['StartGADDR'] = sync.set_start_GADDR(sku_info, pre_BasicsGADDRData)
        param['ProductGADDR'] = sync.set_product_GADDR(sku_info, pre_BasicsGADDRData)
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
    def set_start_GADDR(self, sku_info, pre_BasicsGADDRData):
        try:
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


            return pre_BasicsGADDRData[str(c)]
        except BaseException as err:
            return pre_BasicsGADDRData['宁波']


    # 封装目的地
    def set_product_GADDR(self, sku_info, pre_BasicsGADDRData):
        try:
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

            return pre_BasicsGADDRData[str(cy)]
        except BaseException as err:
            return pre_BasicsGADDRData['日本']

    def set_belong(self, ProductGADDR):
        default_address = {}
        count = len(all_address_belong_data)
        i = 0

        for i in range(count):
            add = {
                'ForeignKeyID': all_address_belong_data[i]['OPUser']['ForeignKeyID'],
                'EmployeeName': all_address_belong_data[i]['OPUser']['EmployeeName'],
                'EmployeeDepartmentID': all_address_belong_data[i]['Department']['_id'],
                'EmployeeDepartmentName': all_address_belong_data[i]['Department']['Name'],
            }

            if str(all_address_belong_data[i]['Address']['ForeignKeyID']) == '000000000000000000000000':
                default_address = add

            if ProductGADDR == all_address_belong_data[i]['Address']['AddressName']:
                return add

        return default_address

    # 获取目的地-私顾计调 对应数据
    def get_orderclaimconfiguration(self):
        global all_address_belong_data
        all_address_belong_data = []

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

        res = col_OrderClaimConfiguration.aggregate(
            [
                {'$match': condition},
                {'$lookup': lookup},
                {'$lookup': lookup2},
                {'$project': project},
                unwind,
            ]
        )

        for one in res:
            all_address_belong_data.append(one)

    def single_query(self, num_iid, times):
        try:
            # -*- coding: utf-8 -*-
            import top.api

            req = top.api.AlitripTravelItemSingleQueryRequest('gw.api.taobao.com', 80)
            req.set_app_info(top.appinfo(const_appkey_main, const_secret_main))

            req.item_id = num_iid
            try:
                resp = req.getResponse(const_sessionkey_main)
                return resp
            except BaseException as err:
                if const_need_print:
                    print(err)

        except BaseException as err:
            if const_need_print:
                print('Exception single_query: ', err)

            if times < 10:
                times += 1

                time.sleep(1)
                if const_need_print:
                    print('第', times, '次重试single_query')
                sync.single_query(num_iid, times)

    def get_num_iid_list(self, post_item_id = 0):
        try:
            result = sync.get_list_post()
            num_iid_list = []
            approve_status_list = {}

            if result['response']['status'] == 701:
                list = json.loads(result['response']['data'])

                if len(list) == 0:
                    pass
                else:
                    for data in list:
                        if post_item_id > 0 and str(post_item_id) != str(data['num_iid']):
                            continue

                        # 检查outer_id
                        if not sync.check_outer_id(data, list):
                            continue

                        num_iid_list.append({
                            'num_iid': data['num_iid'],
                            'outer_id': data['outer_id'],
                        })
                        approve_status_list[str(data['num_iid'])] = data['approve_status']

            if len(num_iid_list) == 0:
                return False
            else:
                return {
                    'num_iid_list': num_iid_list,
                    'approve_status_list': approve_status_list,
                }

        except BaseException as err:
            if const_need_print:
                print('Exception get_num_iid_list: ', err)

            return False

    # 检查outer_id
    def check_outer_id(self, data, list):
        outer_id_exists = True
        if 'outer_id' not in data:
            outer_id_exists = False

        if not data['outer_id']:
            outer_id_exists = False

        if not outer_id_exists:
            log = {
                'Function': 'get_num_iid_list',
                'Subtype': 12,
                'Message': 'item_id: ' + str(data['num_iid']) + ' 的outer_id 字段不存在, 跳过处理',
                'Data': list,
            }
            sync.add_log(log)

            no_outer_id_product_list.append(str(data['num_iid']))

            return False

        if 'shix' not in str(data['outer_id']):
            log = {
                'Function': 'get_num_iid_list',
                'Subtype': 13,
                'Message': 'item_id: ' + str(data['num_iid']) + ' 的outer_id 字段命名不属于视线, 跳过处理',
                'Data': list,
            }
            sync.add_log(log)

            no_outer_id_product_list.append(str(data['num_iid']))

            return False

        return True

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
    def add_log(self, data, not_print=False):
        if not not_print:
            msg = (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'), ': ', data['Message']
            if const_need_print:
                print(msg)

            # dir = 'log'
            # if const_need_print:
            #     if not os.path.exists(dir):
            #         os.makedirs(dir)
            #
            #     log_file_name = dir + '/sync_fliggy_product_to_erp.txt'
            #     with open(log_file_name, 'a') as file_obj:
            #         file_obj.write(str(msg) + " \n")

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

        # col_ChangeLog.insert_one(insert_data)

        log_list.append(InsertOne(insert_data))

    # 更新 PlatformProductNumberConfig 表
    def update_platform_product_number_config(self, num_iid, outer_sku_id_list, title):
        col_PlatformProductNumberConfig.find_one_and_update(
            {
                'DelStatus': 0,
                'ProductNumber': str(num_iid),
            },
            {
                '$set': {
                    'SkuNumbers': outer_sku_id_list,
                    'ProductTitle': title,
                },
            },
            None, None, True
        )

    def insert_final_message(self, change_log_mark = 0):
        log = {
            'Function': 'insert_final_message',
            'Subtype': 15,
            'Message': 'final message',
            'Data': final_message,
        }

        if change_log_mark > 0:
            log['TimeStampFromPHP'] = change_log_mark

        sync.add_log(log, True)

    def toggle_mq_queue(self, status='true'):
        """
        开关mq队列
        """
        post_data = {
            "QueueConfig": "Platform.Price|1|2",
            "Enable": str(status),
            "LimitSecond": "300"
        }

        url = rpc_url + toggle_mq_queue_url

        res = requests.post(
            url=url,
            data=post_data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )

    def del_no_outer_id_product(self):
        """
        @name       删除无商家编码, 商家编码无效的 计划和价格方案
        """
        log = {
            'Function': 'del_no_outer_id_product',
            'Subtype': 17,
            'Message': '删除无商家编码, 商家编码无效的 计划和价格方案',
            'Data': no_outer_id_product_list,
        }
        sync.add_log(log)

        temp_condition = {
            'FliggyResetTime': {'$exists': False},
            'fliggy_item_id': {'$in': no_outer_id_product_list},
            'IsDel': 0,
        }

        col_ProductPlan.update_many(
            temp_condition,
            {
                "$set": {
                    # "IsDel": 1,
                    # "DelTime": now_iso_date,
                    # "DelSource": 'NoOuterID',
                    "FliggyResetTime": now_iso_date,
                    # 'PlanNumber': 0,
                    'MorethanNumber': 0,
                    'fliggy_stock': '0',
                }
            },
        )

        col_ProductPlanPrice.update_many(
            temp_condition,
            {
                "$set": {
                    # "IsDel": 1,
                    # "DelTime": now_iso_date,
                    # "DelSource": 'NoOuterID',
                    "FliggyResetTime": now_iso_date,
                    # 'PlanNumber': 0,
                    'MorethanNumber': 0,
                    'fliggy_stock': '0',
                }
            },
        )

        sync.del_unrelated_in_PlatformProductNumberConfig()

    def del_unrelated_in_PlatformProductNumberConfig(self):
        """
        PlatformProductNumberConfig 表删除
        """
        temp_condition = {
            'ProductNumber': {'$in': no_outer_id_product_list},
            'DelStatus': 0,
        }

        col_PlatformProductNumberConfig.update_many(
            temp_condition,
            {
                "$set": {
                    'DelStatus': 1,
                }
            },
        )

    def set_modified_product_list(self):
        global modified_product_list
        modified_product_list = {}

        temp_condition = {
            'IsDel': 0,
            'AliField.item_id': {'$exists': True},
        }
        temp_project = {
            'AliField.modified': 1,
            'AliField.item_id': 1,
        }
        product_list = col_ProductTeamtour.find(temp_condition, temp_project)

        for p in product_list:
            modified_product_list[str(p['AliField']['item_id'])] = str(p['AliField']['modified'])



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


sync = sync()
try:
    print((datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S') + ' Python Server Start')
    start = time.perf_counter()

    global final_message
    global const_need_print
    global ignore_modified_time
    global log_list

    const_need_print = True
    final_message = []
    log_list = []
    change_log_mark = 0

    try:
        if int(sys.argv[1]) > 0:
            change_log_mark = int(sys.argv[1])
            const_need_print = False
    except BaseException as err:
        pass

    ignore_modified_time = False
    try:
        if str(sys.argv[3]) == 'ignore':
            ignore_modified_time = True
            if const_need_print:
                print('ignore mode')
    except BaseException as err:
        pass

    sync.init_fliggy_param()

    post_item_id = 0
    try:
        if int(sys.argv[2]) > 0:
            post_item_id = int(sys.argv[2])
            # list_res = {
            #     'num_iid_list': [
            #         {
            #             'num_iid': int(sys.argv[2]),
            #             'outer_id': 'shixian',
            #         },
            #     ],
            #     'approve_status_list': {
            #         str(sys.argv[2]): 'onsale'
            #     }
            # }
    except BaseException as err:
        pass

    # post_item_id = 592200971406

    list_res = sync.get_num_iid_list(post_item_id)

    sync.set_modified_product_list()
    sync.get_orderclaimconfiguration()

    if not list_res:
        log = {
            'Function': 'main',
            'Subtype': 2,
            'Message': '列表为空',
            'Result': list_res,
        }
        sync.add_log(log)
        final_message.append('视线业务产品列表为空, 不进行处理')
    else:
        sync.toggle_mq_queue('false')

        environment = 3

        if environment == 1:
            sync.main(1, {
                'num_iid': '5981457204671',
                'outer_id': '598145720467',
            })

        if environment == 2:
            # # 方法一
            i = 0
            for goods in list_res['num_iid_list']:
                i += 1

                try:
                    _thread.start_new_thread(sync.main, ("Thread-" + str(i), goods))
                except:
                    print("Error: 无法启动线程")

                time.sleep(0.1)

            time.sleep(8)

        if environment == 3:
            # 方法二
            threads = []
            i = 0
            temp_len = len(list_res['num_iid_list'])
            no = range(temp_len)
            no1 = range(temp_len + 1)

            for i in no:
                t = threading.Thread(target=sync.main, args=("Thread-" + str(i), list_res['num_iid_list'][i]))
                threads.append(t)

            # t = threading.Thread(target=sync.del_no_outer_id_product, args=("Thread-D",))
            # threads.append(t)

            for i in no:  # start threads 此处并不会执行线程，而是将任务分发到每个线程，同步线程。等同步完成后再开始执行start方法
                threads[i].start()
                time.sleep(0.1)
            for i in no:  # join()方法等待线程完成
                threads[i].join()

    sync.del_no_outer_id_product()
    sync.insert_final_message(change_log_mark)
    col_ChangeLog.bulk_write(log_list)

    if const_need_print:
        elapsed = (time.perf_counter() - start)
        print("Time used:", elapsed)
finally:
    sync.toggle_mq_queue('true')
    print('final result:100')
