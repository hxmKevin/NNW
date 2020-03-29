# coding: utf-8
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))

import requests
import random
from pika.compat import xrange
from pymongo import UpdateOne, InsertOne
from common import handle_mongodb
from common.readconfig import ReadConfig
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
from common import send_email

cst_tz = timezone('Asia/Shanghai')


class Check:
    def __init__(self):
        self.partner_id = 'taobao-sdk-python-20190912'
        self.main_base_url = 'http://11186295n9.api.taobao.com/router/qm'

        self.system_id = '000000000000000000002251'
        self.IM_URL = str(ReadConfig().get_url("im_url"))
        self.mongodb = handle_mongodb.HandleMongoDB()
        self.mongodb.mongodb_connect()

        self.col_FliggyLog = self.mongodb.select_col('FliggyLog')
        self.col_ProductTeamtour = self.mongodb.select_col('ProductTeamtour')
        self.col_ProductPlan = self.mongodb.select_col('ProductPlan')
        self.col_ProductPlanPrice = self.mongodb.select_col('ProductPlanPrice')
        self.col_AliFliggyStores = self.mongodb.select_col('AliFliggyStores')

        year = datetime.datetime.now().year
        month = datetime.datetime.now().month
        day = datetime.datetime.now().day
        hour = datetime.datetime.now().hour
        minute = datetime.datetime.now().minute
        second = datetime.datetime.now().second
        self.now_iso_date = cst_tz.localize(datetime.datetime(year, month, day, hour, minute, second))

        self.log_list = []
        self.final_message = []
        self.repeat_outer_sku_id = []
        self.outer_sku_id_list = {}

    def main(self):
        try:
            list_res = Check.get_num_iid_list()

            threads = []
            temp_len = len(list_res['num_iid_list'])
            no = range(temp_len)

            for i in no:
                t = threading.Thread(target=Check.check, args=("Thread-" + str(i), list_res['num_iid_list'][i]))
                threads.append(t)

            for i in no:
                threads[i].start()
                time.sleep(0.1)
            for i in no:
                threads[i].join()

            Check.send_email()
        except BaseException as err:
            print('Exception get_list_post: ', err)

    def send_email(self):
        print('final_message-1:')
        print(json_util.dumps(self.final_message, ensure_ascii=False))

        for one in self.repeat_outer_sku_id:
            if one in self.outer_sku_id_list:
                temp_text = ''
                for temp_one in self.outer_sku_id_list[str(one)]:
                    temp_text = temp_text + str(temp_one) + ' '
                self.final_message.append('套餐编码' + one + '存在于多个产品中: ' + temp_text + ', 请检查')

        print('final_message-2:')
        print(json_util.dumps(self.final_message, ensure_ascii=False))

        if self.final_message:
            content = ''

            for msg in self.final_message:
                content = content + msg + "\r\n"

            text = {
                'title': '飞猪产品检查:' + (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'),
                'content': content,
            }
            send_email.EmailSmtp().send('854641898@qq.com', text)

    def single_query(self, num_iid, times):
        try:
            # -*- coding: utf-8 -*-
            import top.api

            req = top.api.AlitripTravelItemSingleQueryRequest('gw.api.taobao.com', 80)
            req.set_app_info(top.appinfo(self.const_appkey_main, self.const_secret_main))

            req.item_id = num_iid
            try:
                resp = req.getResponse(self.const_sessionkey_main)
                return resp
            except BaseException as err:
                print(err)

        except BaseException as err:
            print('Exception single_query: ', err)

            if times < 10:
                times += 1

                time.sleep(1)
                print('第', times, '次重试single_query')
                Check.single_query(num_iid, times)

    def check(self, ThreadName, goods):
        try:
            num_iid = goods['num_iid']

            # print('item_id: ', num_iid, 'main 处理开始')

            temp_res = Check.single_query(num_iid, 0)

            # log = {
            #     'Function': 'main',
            #     'Subtype': 19,
            #     'Message': str(num_iid) + '添加详情日志',
            #     'Data': temp_res,
            # }
            # Check.add_log(log)

            if not temp_res:
                # 记录日志
                temp_msg = 'main: item_id: ' + str(num_iid) + ' 详情查询失败, 请检查或联系管理员'
                print(temp_msg)

                log = {
                    'Function': 'main',
                    'Subtype': 18,
                    'Message': temp_msg,
                    'Result': temp_res,
                }
                Check.add_log(log)
                self.final_message.append(log['Message'])
                return False

            if 'alitrip_travel_item_single_query_response' not in temp_res:
                # 记录日志
                print('main: item_id: ' + str(num_iid) + ' alitrip_travel_item_single_query_response 字段不存在')

                log = {
                    'Function': 'main',
                    'Subtype': 2,
                    'Message': 'main: item_id: ' + str(num_iid) + ' alitrip_travel_item_single_query_response 字段不存在',
                    'Result': temp_res,
                }
                Check.add_log(log)
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
                Check.add_log(log)
                return False

            if not temp_res['travel_item']:
                # 记录日志
                log = {
                    'Function': 'main',
                    'Subtype': 4,
                    'Message': 'main: item_id: ' + str(num_iid) + ' travel_item 字段为空',
                    'Result': temp_res,
                }
                Check.add_log(log)
                return False

            if 'out_id' not in temp_res['travel_item']['base_info']:
                # 记录日志
                log = {
                    'Function': 'main',
                    'Subtype': 5,
                    'Message': 'main: item_id: ' + str(num_iid) + ' out_id(商家编码) 字段不存在, 跳过处理',
                    'Result': temp_res,
                }
                Check.add_log(log)
                self.final_message.append(log['Message'])
                return False

            if not temp_res['travel_item']['base_info']['out_id']:
                # 记录日志
                log = {
                    'Function': 'main',
                    'Subtype': 6,
                    'Message': 'main: item_id: ' + str(num_iid) + ' out_id(商家编码) 字段为空, 跳过处理',
                    'Result': temp_res,
                }
                Check.add_log(log)
                self.final_message.append(log['Message'])
                return False

            detail = temp_res['travel_item']
            detail['base_info']['out_id'] = str(detail['base_info']['out_id'])
            detail['item_id'] = str(detail['item_id'])
            del temp_res

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
            if not Check.check_outer_sku_id_repeat(detail, sku_infos):
                return False

        except BaseException as err:
            log = {
                'Function': 'main',
                'Subtype': 8,
                'Message': '请联系管理员检查, Exception main: item_id: ' + str(num_iid) + ': ' + str(err),
            }
            self.final_message.append(log['Message'])
            Check.add_log(log)
        finally:
            return 'success'

    # 检查outer_sku_id 是否存在重复
    def check_outer_sku_id_repeat(self, detail, sku_infos):
        temp_list = []
        for sku_info in sku_infos:
            try:
                outer_sku_id = str(sku_info['outer_sku_id'])
                if outer_sku_id in self.outer_sku_id_list:
                    # self.final_message.append('套餐编码' + outer_sku_id + '存在于多个产品中, 请检查')
                    self.outer_sku_id_list[outer_sku_id].append(str(detail['item_id']))
                    self.repeat_outer_sku_id.append(outer_sku_id)
                else:
                    self.outer_sku_id_list[outer_sku_id] = [
                        str(detail['item_id'])
                    ]

                # self.outer_sku_id_list.append(outer_sku_id)

                if outer_sku_id in temp_list:
                    log = {
                        'Function': 'check_outer_sku_id_repeat',
                        'Subtype': 10,
                        'Message': 'item_id: ' + str(detail['item_id']) + ' ' + str(
                            outer_sku_id) + ' 存在相同套餐编码, 请检查',
                    }
                    Check.add_log(log)
                    self.final_message.append(log['Message'])
                    return False

                if not outer_sku_id:
                    log = {
                        'Function': 'check_outer_sku_id_repeat',
                        'Subtype': 16,
                        'Message': 'item_id: ' + str(detail['item_id']) + ' ' + str(
                            outer_sku_id) + ' 存在空套餐编码, 请检查',
                    }
                    Check.add_log(log)
                    self.final_message.append(log['Message'])
                    return False

                temp_list.append(outer_sku_id)
            except BaseException as err:
                continue

        return True

    def add_log(self, data, not_print=False):
        if not not_print:
            msg = (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'), ': ', data['Message']
            print(msg)

        insert_data = {
            'userID': ObjectId('5d8c5c2228142658548b456a'),
            'userName': '系统管理员Online',
            'departmentID': ObjectId('000000000000000000000781'),
            'departmentName': '浙江恒越信息科技有限公司',
            'time': self.now_iso_date,
            'type': 1,
            'data': {
                'Language': 'python3',
                'Class': 'Check',
            },
        }

        insert_data['data'].update(data)

        self.log_list.append(InsertOne(insert_data))

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

        res = self.col_AliFliggyStores.find(temp_condition)

        if not res:
            log = {
                'Function': 'init_fliggy_param',
                'Subtype': 1,
                'Message': '获取飞猪店铺数据失败',
                'Data': {
                    'Result': res,
                },
            }
            Check.add_log(log)
            exit()

        for data in res:
            if str(data['_id']) == '000000000000000000000001':
                LC_SHOP = data
                continue

            if str(data['_id']) == '000000000000000000000002':
                MAIN_SHOP = data
                continue

        self.const_appkey_main = str(MAIN_SHOP['AppId'])
        self.const_secret_main = str(MAIN_SHOP['AppSecret'])
        self.const_sessionkey_main = str(MAIN_SHOP['Session'])

    def get_list_post(self):
        try:
            param = {}
            now_str = (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S')

            param['app_key'] = self.const_appkey_main
            param['method'] = 'iflying.shixian.list.get'
            param['v'] = '2.0'
            param['target_app_key'] = param['app_key']
            param['partner_id'] = self.partner_id
            param['timestamp'] = str(now_str)
            param['sign_method'] = 'md5'
            param['format'] = 'json'
            param[
                'RequestData'] = '{"orderType":2,"banner":"","cid":50278002,"seller_cids":"","orderBy":"","startModified":"","endModified":""}'

            sign = Check.sign(self.const_secret_main, param)
            url = self.main_base_url + '' + '?RequestData=' + urllib.parse.quote(param['RequestData']) + '&app_key=' + param[
                'app_key'] + '&method=' + param['method'] + '&v=' + param[
                      'v'] + '&sign=' + sign + '&timestamp=' + urllib.parse.quote(
                param['timestamp']) + '&target_app_key=' + param['target_app_key'] + '&partner_id=' + param[
                      'partner_id'] + '&format=' + param['format'] + '&sign_method=' + param['sign_method']
            r = requests.post(url)
            return r.json()

        except BaseException as err:
            print('Exception get_list_post: ', err)

    def get_num_iid_list(self):
        try:
            result = Check.get_list_post()
            num_iid_list = []
            approve_status_list = {}

            if result['response']['status'] == 701:
                list = json.loads(result['response']['data'])

                if len(list) == 0:
                    pass
                else:
                    for data in list:
                        # 检查outer_id
                        if not Check.check_outer_id(data):
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
            print('Exception get_num_iid_list: ', err)
            return False

    # 检查outer_id
    def check_outer_id(self, data):
        outer_id_exists = True
        if 'outer_id' not in data:
            outer_id_exists = False

        if not data['outer_id']:
            outer_id_exists = False

        if not outer_id_exists:
            # log = {
            #     'Function': 'get_num_iid_list',
            #     'Subtype': 12,
            #     'Message': 'item_id: ' + str(data['num_iid']) + ' 的outer_id 字段不存在, 跳过处理',
            #     'Data': list,
            # }
            # Check.add_log(log)

            return False

        if 'shix' not in str(data['outer_id']):
            # log = {
            #     'Function': 'get_num_iid_list',
            #     'Subtype': 13,
            #     'Message': 'item_id: ' + str(data['num_iid']) + ' 的outer_id 字段命名不属于视线, 跳过处理',
            #     'Data': list,
            # }
            # Check.add_log(log)

            return False

        return True

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


if __name__ == '__main__':
    try:
        print((datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S') + ' Python Server Start')
        start = time.perf_counter()

        Check = Check()
        Check.init_fliggy_param()

        Check.main()
    except BaseException as err:
        print('__main__ exception: ')
        print(err)
    finally:
        elapsed = (time.perf_counter() - start)
        print("Python Complete, Time used:", elapsed)
