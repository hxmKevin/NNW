# coding: utf-8
"""
处理会员信息发生变化后, 一些非即时性的处理
2020.1.15       会员存在身份证 且 没有生日 则 从身份证提取生日
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
import pika

cst_tz = timezone('Asia/Shanghai')


class MQCustomers:
    def __init__(self):
        self.system_id = '000000000000000000002251'
        self.IM_URL = str(ReadConfig().get_url("im_url"))
        self.mongodb = handle_mongodb.HandleMongoDB()
        self.mongodb.mongodb_connect()

        self.col_MarketingCoupon = self.mongodb.select_col('MarketingCoupon')
        self.col_CustomerLevels = self.mongodb.select_col('CustomerLevels')
        self.col_Customers = self.mongodb.select_col('Customers')
        self.col_CustomerCouponLog = self.mongodb.select_col('CustomerCouponLog')

        self.now_timestamp = int(time.time())
        self.today_start_timestamp = int(time.mktime(time.strptime(str(datetime.date.today()), '%Y-%m-%d'))) - 1 + 1

        year = datetime.datetime.now().year
        one_year_later = year + 1
        month = datetime.datetime.now().month
        day = datetime.datetime.now().day
        hour = datetime.datetime.now().hour
        minute = datetime.datetime.now().minute
        second = datetime.datetime.now().second
        self.now_iso_date = cst_tz.localize(datetime.datetime(year, month, day, hour, minute, second))
        self.one_year_later_iso_date = cst_tz.localize(datetime.datetime(one_year_later, month, day, hour, minute, second))

    def ConsumerCallback(self, channel, method, properties, body):
        print((datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'), " Received: ", body)
        # data = json.loads(body.decode('utf-8'))
        data = body.decode()
        customer_id = str(data)
        customer_data = MQC.query_customer(customer_id)

        if not customer_data:
            print('查询不到会员数据:' + customer_id)
        else:
            MQC.send_coupon(data, customer_data)

            self.fetch_birthday_from_id_card(customer_id, customer_data)

    def fetch_birthday_from_id_card(self, customer_id, customer_data):
        """
        会员存在身份证 且 没有生日 则 从身份证提取生日
        :param customer_id:
        :param customer_data:
        :return:
        """
        birth_year = 0
        birth_month = 0
        birth_day = 0
        birth_exists = False
        fetch_birth_correct = False

        try:
            if str(customer_data['Enlarge']['CertificatesType']['ForeignKeyID']) == '000000000000000000000001':
                idc = customer_data['Enlarge']['CustomerDocumentNumber']

                birth_year = int(idc[6:10])
                birth_month = int(idc[10:12])
                birth_day = int(idc[12:14])
        except:
            pass

        if birth_year > 0 and birth_month > 0 and birth_month < 13 and birth_day > 0 and birth_day < 32:
            fetch_birth_correct = True

        try:
            if customer_data['Enlarge']['CustomerBirth']:
                birth_exists = True
        except:
            pass

        if fetch_birth_correct and not birth_exists:
            temp_condition = {
                '_id': ObjectId(customer_id),
            }

            operation = {
                '$set': {
                    'Enlarge.CustomerBirth': cst_tz.localize(datetime.datetime(birth_year, birth_month, birth_day, 12, 0, 0)),
                }
            }

            self.col_Customers.update_one(
                temp_condition,
                operation,
            )

    def query_customer(self, customer_id):
        """
        查询会员信息
        """
        temp_condition = {
            'DelStatus': 0,
            '_id': ObjectId(customer_id),
        }

        res = self.col_Customers.find_one(temp_condition)

        return res

    def send_coupon(self, customer_id, customer_data):
        print('')
        print((datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S') + ' send_coupon Start')
        print('customer_id: ' + customer_id)

        try:
            customer_level_id = str(customer_data['Enlarge']['CustomerLevelID'])
        except BaseException as err:
            customer_level_id = 'none'

        print('customer_level_id: ' + customer_level_id)

        if customer_level_id == '5ddb467ebba63906008b4577':
            print('银卡会员')
            pre_send_coupon_list = ['5de4733ab0ae1c7b1b765c99']
        elif customer_level_id == '5ddb478abba63908008b4576':
            print('金卡会员')
            pre_send_coupon_list = ['5de4a606b0ae1c153c765cab', '5de4a5c2b0ae1c123c765cb4', '5de4a42ab0ae1cbd3b765c94']
        elif customer_level_id == '5ddb47a6bba63907008b4578':
            print('钻石会员')
            pre_send_coupon_list = ['5de4a7c5b0ae1c723e765ca0', '5de4a771b0ae1cb93f765c7f', '5de4a717b0ae1cfa3b765cce']
        else:
            return False

        pre_send_coupon_list_objectid = []
        for one in pre_send_coupon_list:
            pre_send_coupon_list_objectid.append(ObjectId(one))

        condition = {
            'CustomerID': ObjectId(customer_id),
            'CouponID': {'$in': pre_send_coupon_list_objectid},
        }

        projection = {
            '_id': 1,
        }

        res = self.col_CustomerCouponLog.find(condition, projection)

        for one in res:
            print('已发优惠券')
            return False

        for coupon_id in pre_send_coupon_list:
            MQC.send_coupon_deep(customer_id, coupon_id)
            MQC.add_send_coupon_log(customer_id, customer_level_id, coupon_id)

    def send_coupon_deep(self, customer_id, coupon_id):
        print((datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S') + ' send_coupon_deep Start')
        coupon_data = MQC.query_coupon(coupon_id)

        if '_id' not in coupon_data:
            return False

        # 发行数+1
        MQC.inc_sale_volume(coupon_id)

        RelatedService = {}
        if 'RelatedService' in coupon_data:
            try:
                RelatedService = {
                    'ForeignKeyID': ObjectId(coupon_data['RelatedService']['_id']),
                    'ServiceMobile': coupon_data['RelatedService']['ServiceMobile'],
                    'ServiceNickName': coupon_data['RelatedService']['ServiceNickName'],
                    'ServiceCompanyName': coupon_data['RelatedService']['ServiceCompanyName'],
                    'ServiceAddr': coupon_data['RelatedService']['ServiceAddr'],
                }
            except BaseException as err:
                pass

        try:
            CouponType = coupon_data['CouponType']
        except BaseException as err:
            CouponType = None

        try:
            Price = coupon_data['Price']
        except BaseException as err:
            Price = 0.0

        try:
            FullReductionPrice = coupon_data['FullReductionPrice']
        except BaseException as err:
            FullReductionPrice = 0.0

        try:
            ProductType = coupon_data['ProductType']
        except BaseException as err:
            ProductType = None

        try:
            LineType = coupon_data['LineType']
        except BaseException as err:
            LineType = None

        try:
            CouponMode = coupon_data['CouponMode']
        except BaseException as err:
            CouponMode = None

        try:
            ShareStatus = coupon_data['ShareStatus']
        except BaseException as err:
            ShareStatus = 0

        try:
            StartTime = coupon_data['StartTime']
        except BaseException as err:
            StartTime = None

        try:
            EndTime = coupon_data['EndTime']
        except BaseException as err:
            EndTime = None

        try:
            ValidType = coupon_data['ValidType']
        except BaseException as err:
            ValidType = 0

        try:
            Duration = coupon_data['Duration']
        except BaseException as err:
            Duration = 0

        try:
            PicUrl = coupon_data['PicUrl']
        except BaseException as err:
            PicUrl = 0

        couponSaveData = {
            'CouponNo': ObjectId(),
            'ForeignKeyID': coupon_data['_id'],
            'Title': coupon_data['Title'],
            'Directions': coupon_data['Directions'],
            'CouponType': CouponType,
            'Price': Price,
            'FullReductionPrice': FullReductionPrice,
            'ProductType': ProductType,
            'LineType': LineType,
            'CouponMode': CouponMode,
            'ShareStatus': ShareStatus,
            'StartTime': StartTime,
            'EndTime': EndTime,
            'ValidType': ValidType,
            'Duration': Duration,
            'CouponCategory': coupon_data['CouponCategory'],
            'RelatedService': RelatedService,
            'Status': 0,
            'GetTime': self.now_iso_date,
            'CardNo': MQC.get_product_code(15),
            'PicUrl': PicUrl,
        }

        temp_condition = {
            '_id': ObjectId(customer_id),
        }

        operation = {
            '$push': {
                'Coupons': couponSaveData,
            }
        }

        self.col_Customers.update_one(
            temp_condition,
            operation,
        )

    def add_send_coupon_log(self, customer_id, customer_level_id, coupon_id):
        """
        添加发放记录
        """
        op_data = {
            'CustomerID': ObjectId(customer_id),
            'CustomerLevelID': ObjectId(customer_level_id),
            'CouponID': ObjectId(coupon_id),
            'Type': 3,
            'status': 0,
            'time': self.now_iso_date,
            'nexttime': self.one_year_later_iso_date,
        }

        self.col_CustomerCouponLog.insert_one(op_data)

    def get_product_code(self, len_limit):
        chars_array = [
            "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
            "A", "B", "C", "D", "E", "F", "G",
            "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R",
            "S", "T", "U", "V", "W", "X", "Y", "Z"
        ]

        charsLen = len(chars_array) - 1
        outputstr = ""

        i = 0
        for i in range(len_limit-1):
            outputstr = outputstr + chars_array[random.randint(0, charsLen)]

        return outputstr

    def inc_sale_volume(self, coupon_id):
        """
        发行数+1
        """
        update = {
            '$inc': {
                'SaleVolume': 1,
            }
        }

        temp_condition = {
            '_id': ObjectId(coupon_id)
        }
        self.col_MarketingCoupon.update_one(
            temp_condition,
            update,
        )

    def query_coupon(self, coupon_id):
        """
        查询coupon信息
        """
        temp_condition = {
            'DelStatus': 0,
            '_id': ObjectId(coupon_id),
        }

        res = self.col_MarketingCoupon.find_one(temp_condition)

        return res

    def main(self):
        # environment = 'test'
        environment = 'production'

        if environment == 'test':
            credentials = pika.PlainCredentials("test", "123456")
            parameters = pika.ConnectionParameters(host="172.16.31.241",
                                                   virtual_host='/',
                                                   credentials=credentials)
            connection = pika.BlockingConnection(parameters)  # 连接 RabbitMQ

            channel = connection.channel()  # 创建频道

            queue = channel.queue_declare(queue='kevin_test', durable=True)  # 声明或创建队列

            # no_ack=True 开启自动确认，不然消费后的消息会一直留在队列里面
            # no_ack = no_manual_ack = auto_ack；不手动应答，开启自动应答模式
            channel.basic_consume('kevin_test', MQC.ConsumerCallback, True)
        else:
            MQ = ReadConfig()
            user = str(MQ.get_mq("user"))
            pwd = str(MQ.get_mq("pwd"))
            ip = str(MQ.get_mq("ip"))

            credentials = pika.PlainCredentials(user, pwd)
            parameters = pika.ConnectionParameters(host=ip,
                                                   virtual_host='/',
                                                   credentials=credentials)
            connection = pika.BlockingConnection(parameters)  # 连接 RabbitMQ

            channel = connection.channel()  # 创建频道

            queue = channel.queue_declare(queue='Erp.Customer.FollowUp', durable=True)  # 声明或创建队列

            # no_ack=True 开启自动确认，不然消费后的消息会一直留在队列里面
            # no_ack = no_manual_ack = auto_ack；不手动应答，开启自动应答模式
            channel.basic_consume('Erp.Customer.FollowUp', MQC.ConsumerCallback, True)

        print('Wait Message ...')

        channel.start_consuming()


if __name__ == '__main__':
    start = time.perf_counter()
    print((datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S') + ' Python Server Start')
    MQC = MQCustomers()

    try:
        MQC.main()
        # MQC.ConsumerCallback(1, 1, 1, '5de5e37e281426eb458b456c')
    except BaseException as err:
        print('__main__ exception: ')
        print(err)
    finally:
        elapsed = (time.perf_counter() - start)
        print("Python Server Complete Time used:", elapsed)
