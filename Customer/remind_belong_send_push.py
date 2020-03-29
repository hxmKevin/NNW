# coding: utf-8
"""
http://git.iflying.com/Business/ERP/erp/issues/304
团队二维码微信公众号推送模板和提醒功能
提醒计调向客户发送微信推送
"""

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import requests
import random
from pika.compat import xrange
from pymongo import UpdateOne
from common import handle_mongodb
from common.readconfig import ReadConfig
from bson import json_util
from bson.objectid import ObjectId
import time
import datetime
import json
from pytz import timezone

cst_tz = timezone('Asia/Shanghai')


class Send:
    def __init__(self):
        self.system_id = '000000000000000000002251'
        self.IM_URL = str(ReadConfig().get_url("im_url"))
        self.mongodb = handle_mongodb.HandleMongoDB()
        self.mongodb.mongodb_connect()

        self.col_Personnel = self.mongodb.select_col('Personnel')
        self.col_Department = self.mongodb.select_col('Department')
        self.col_Team = self.mongodb.select_col('Team')
        self.col_ProductTeamtour = self.mongodb.select_col('ProductTeamtour')
        self.col_ProductIndependenttravel = self.mongodb.select_col('ProductIndependenttravel')
        self.col_ProductCruise = self.mongodb.select_col('ProductCruise')
        self.col_ProductSelfdrivingtour = self.mongodb.select_col('ProductSelfdrivingtour')
        self.col_BasicsVisa = self.mongodb.select_col('BasicsVisa')
        self.col_BasicsScenic = self.mongodb.select_col('BasicsScenic')
        self.col_ProductCustomtour = self.mongodb.select_col('ProductCustomtour')
        self.col_Orders = self.mongodb.select_col('Orders')
        self.col_OrderTourists = self.mongodb.select_col('OrderTourists')

        self.now_timestamp = int(time.time())
        self.today_start_timestamp = int(time.mktime(time.strptime(str(datetime.date.today()), '%Y-%m-%d'))) - 1 + 1

        self.cruise_title = ['地中海邮轮', '北欧邮轮', '澳新邮轮', '中东邮轮', '北美邮轮', '加勒比海邮轮', '南美邮轮', '南极邮轮', '北极邮轮', '环球邮轮']
        self.foreign_title = ['欧洲', '北美洲', '美洲', '南美洲', '中美洲', '大洋洲', '澳洲', '非洲', '中东非洲', '南非']
        self.china_title = ['中国', '山东']
        self.circum_title = ['浙江', '江西', '江苏', '安徽', '福建', '上海']
        self.china_special = ['香港', '澳门', '台湾']

    def main(self):
        team_list = Send.query_team_list()
        messages = []
        sent_list = []

        for team in team_list:
            str_StartTime = str(team['StartTime'])
            timestamp = (int(time.mktime(time.strptime(str_StartTime, "%Y-%m-%d %H:%M:%S"))) + 8 * 3600)
            str_date = (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp)).split( ))[0]
            StartTime_timestamp = int(time.mktime(time.strptime(str_date, "%Y-%m-%d")))

            if StartTime_timestamp < self.today_start_timestamp:
                continue

            not_include_4 = False
            total_number = 0
            for temp_order in team['Orders']:
                if int(temp_order['OrderSourceTypeID']) != 4:
                    not_include_4 = True

                product_id = str(temp_order['ProductID'])
                order_type = str(temp_order['OrderType']['ForeignKeyID'])

                try:
                    TotalNumber = int(temp_order['TotalNumber'])
                except BaseException as err:
                    TotalNumber = 0

                total_number = total_number + TotalNumber

            if not not_include_4:
                print(str(team['_id']) + ' 全部是同行, 不作处理')
                continue

            if total_number > 0:
                pass
            else:
                print(str(team['_id']) + ' 关联订单总人数为0, 不作处理')
                continue

            ''' 判断订单游客是否全部退团 作用应该跟 "关联订单总人数为0" 重复 '''
            if not Send.judge_tourist_status(team['Orders']):
                print(str(team['_id']) + ' 不存在正常状态游客, 不作处理')
                continue

            res = Send.query_product(product_id, order_type)
            team_id = str(team['_id'])

            if not res:
                print(str(team['_id']) + ' 查不到产品信息, 不作处理')
                continue

            if StartTime_timestamp == self.today_start_timestamp:
                date_minus = 0
            else:
                date_minus = int((StartTime_timestamp - self.today_start_timestamp)/86400)

            area_type = Send.get_area_type(res)

            need_send = False
            if area_type == 1 and (date_minus == 0 or date_minus == 1 or date_minus == 2):
                need_send = True

            if area_type == 2 and (date_minus == 0 or date_minus == 2 or date_minus == 3 or date_minus == 7):
                need_send = True

            if area_type == 3 and (
                    date_minus == 0 or date_minus == 2 or date_minus == 3 or date_minus == 7 or date_minus == 10 or date_minus == 20):
                need_send = True

            if area_type == 4 and (date_minus == 0 or date_minus == 2 or date_minus == 3 or date_minus == 7 or date_minus == 20):
                need_send = True

            if need_send:
                text = '团队:' + str(team['TeamName']) + ', 编号: ' + str(team['TeamNo']) + ' 还有' + str(
                    date_minus) + '天出发, 请向客户发送微信公众号推送'
                text = text.replace("还有0天", "今天")
                recver = str(team['DeployInfo']['ForeignKeyID'])
                recver_name = str(team['DeployInfo']['EmployeeName'])

                temp_msg = {
                    'sender': str(self.system_id),
                    'recver': recver,
                    'title': text,
                    'content': text,
                    'targetId': team_id,
                    'messageType': 1,
                    'type': 19,
                    'subType': 119006,
                    'hasBusinessId': False,
                }

                messages.append(temp_msg)
                sent_list.append({
                    'team_id': team_id,
                    'personnel_id': recver,
                    'personnel_name': recver_name,
                    'TeamNo': str(team['TeamNo']),
                    'days': str(date_minus),
                })

                if len(messages) > 20:
                    Send.send_message(messages)
                    messages = []

        if len(messages) > 0:
            Send.send_message(messages)

        print('共' + str(len(sent_list)) + '人收到通知:')
        for one in sent_list:
            print('team_id: ' + one['team_id'] + ' personnel: ' + one['personnel_id'] + ': ' +
                  one['personnel_name']+' TeamNo: ' + one['TeamNo']+' 提前天数: ' + one['days'])

    def judge_tourist_status(self, Orders):
        """
        判断游客是否全部退团
        :return     True-存在正常状态游客   False-不存在正常状态游客
        """
        status_list = []

        for order in Orders:
            order_id = str(order['_id'])
            order_tourists_list = Send.query_order_tourists_list(order_id)

            if order_tourists_list:
                for temp_one in order_tourists_list:
                    try:
                        TouristStatus = str(temp_one['TouristStatus'])
                    except BaseException:
                        TouristStatus = '9'
                        
                    status_list.append(TouristStatus)

        if '1' in status_list:
            return True

        return False

    def query_order_tourists_list(self, order_id):
        """
        查询 订单游客信息
        """
        temp_condition = {
            'DelStatus': 0,
            'OrderID': ObjectId(order_id),
        }

        temp_project = {
            'TouristStatus': 1,
        }

        res = self.col_OrderTourists.find(temp_condition, temp_project)

        return res

    def get_iso_date_yesterday(self):
        """
        获取昨天的isodate
        """
        timestamp_yesterday = self.now_timestamp - 86400
        str_yesterday = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp_yesterday))
        datetime_yesterday = datetime.datetime.strptime(str_yesterday, "%Y-%m-%d %H:%M:%S")
        year_yesterday = datetime_yesterday.year
        month_yesterday = datetime_yesterday.month
        day_yesterday = datetime_yesterday.day

        return cst_tz.localize(datetime.datetime(year_yesterday, month_yesterday, day_yesterday, 0, 0, 0))

    def get_iso_date_21_days_later(self):
        """
        获取21天后的isodate
        """
        timestamp_21_days_later = self.now_timestamp + 21 * 86400
        str_21_days_later = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp_21_days_later))
        datetime_21_days_later = datetime.datetime.strptime(str_21_days_later, "%Y-%m-%d %H:%M:%S")
        year_21_days_later = datetime_21_days_later.year
        month_21_days_later = datetime_21_days_later.month
        day_21_days_later = datetime_21_days_later.day

        return cst_tz.localize(
            datetime.datetime(year_21_days_later, month_21_days_later, day_21_days_later, 23, 59, 59))

    def get_area_type(self, product_data):
        """
        判断出境长线短线的方法
        1是周边，2是国内，3是出境短线，4是出境长线
        """
        parent_name_list = []
        try:
            Parents = product_data['ProductGADDR']['Parents']
            for one in Parents:
                parent_name_list.append(one['Name'])
        except BaseException as err:
            pass

        Title = product_data['ProductGADDR']['Title']

        area_type = 3
        if Title in self.cruise_title:
            area_type = 4

        if parent_name_list:
            if (set(parent_name_list) & set(self.foreign_title)):
                area_type = 4

            if (set(parent_name_list) & set(self.china_title)):
                area_type = 2

            if (set(parent_name_list) & set(self.circum_title)):
                area_type = 1

            if (set(parent_name_list) & set(self.china_special)):
                area_type = 3

        return area_type

    def query_product(self, product_id, order_type):
        """
        查询产品信息
        """
        col = Send.select_product_col(order_type)
        if not col:
            return False

        temp_condition = {
            'IsDel': 0,
            '_id': ObjectId(product_id),
        }

        temp_project = {
            'ProductGADDR': 1,
        }

        res = col.find_one(temp_condition, temp_project)

        return res

    def select_product_col(self, ProductTypeID):
        temp_col = None

        if str(ProductTypeID) == '000000000000000000000001' \
                or str(ProductTypeID) == '000000000000000000000032'\
                or str(ProductTypeID) == '000000000000000000000023':
            temp_col = self.col_ProductTeamtour

        if str(ProductTypeID) == '000000000000000000000012':
            temp_col = self.col_ProductIndependenttravel

        if str(ProductTypeID) == '000000000000000000000018':
            temp_col = self.col_ProductCruise

        if str(ProductTypeID) == '000000000000000000000013':
            temp_col = self.col_ProductSelfdrivingtour

        if str(ProductTypeID) == '000000000000000000000002':
            temp_col = self.col_BasicsVisa

        if str(ProductTypeID) == '000000000000000000000003':
            temp_col = self.col_BasicsScenic

        if str(ProductTypeID) == '000000000000000000000017':
            temp_col = self.col_ProductCustomtour

        if not temp_col:
            print('ProductType 无法识别: ' + str(ProductTypeID))
            return False

        return temp_col

    def send_message(self, message):
        """
        发送通知
        """
        try:
            msg = {
                'message': message
            }
            json_msg = json.dumps(msg)
            post_data = 'token=&data=' + json_msg + '&notCheckSender=true'

            url = str(self.IM_URL) + 'crmSendRemindBatch'

            res = requests.post(
                url=url,
                data=post_data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )

            print('发送给客户打标签通知: ')
            print(message)
            print(res.text)
        except BaseException as err:
            print('main send_message: ' + err)

    def query_team_list(self):
        iso_date_yesterday = Send.get_iso_date_yesterday()
        iso_date_21_days_later = Send.get_iso_date_21_days_later()

        temp_condition = {
            'IsDel': 0,
            'TeamStatus': 1,
            'CompanyInfo.ForeignKeyID': {
                '$in': [
                    ObjectId('000000000000000000000001'),
                    ObjectId('000000000000000000000028')
                ]
            },
            'ProductType.ForeignKeyID': {
                '$in': [
                    ObjectId('000000000000000000000001'),
                    ObjectId('000000000000000000000018')
                ]
            },
            'StartTime': {
                '$gte': iso_date_yesterday,
                '$lte': iso_date_21_days_later,
            },
            'OrderCount': {
                '$gt': 0,
            }
        }

        lookup = {
            'from': "Orders",
            "localField": "_id",
            "foreignField": "FinancesEnlarge.TeamID",
            "as": "Orders",
        }

        temp_condition_2 = {
            'Orders.OrderStatus': {"$in": [2, 3]}
        }

        temp_project = {
            'StartTime': 1,
            'DeployInfo': 1,
            'TeamNo': 1,
            'TeamName': 1,
            'Orders._id': 1,
            'Orders.OrderType': 1,
            'Orders.ProductID': 1,
            'Orders.OrderNo': 1,
            'Orders.OrderSourceTypeID': 1,
            'Orders.TotalNumber': 1,
            'Orders.OrderStatus': 1,
        }

        temp_project_2 = {
            'StartTime': 1,
            'DeployInfo': 1,
            'TeamNo': 1,
            'TeamName': 1,
            'Orders': {
                "$filter": {
                    "input": "$Orders",
                    "as": "item",
                    "cond": {
                        "$in": ["$$item.OrderStatus", [2, 3]]
                    }
                }
            }
        }

        unwind = {
            '$unwind': '$Orders',
        }

        team_list = self.col_Team.aggregate(
            [
                {'$match': temp_condition},
                {'$lookup': lookup},
                {'$match': temp_condition_2},
                {'$project': temp_project},
                {'$project': temp_project_2},
                # unwind,
            ]
        )

        return team_list


if __name__ == '__main__':
    print((datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S') + ' Python Server Start')
    start = time.perf_counter()
    Send = Send()

    try:
        Send.main()
    except BaseException as err:
        print('__main__ exception: ')
        print(err)
    finally:
        elapsed = (time.perf_counter() - start)
        print("Python Server Complete Time used:", elapsed)
