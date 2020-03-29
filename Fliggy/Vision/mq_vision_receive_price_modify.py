# encoding: utf-8
# author: walker
# date: 2018-01-31
# summary: 接收方/消费者

import os, sys, time
import pika
import xmltodict
import json
import requests
import top.api
import datetime
from pytz import timezone
import pymongo
from bson.objectid import ObjectId
import mongodb_config
from constants import const


# 接收处理消息的回调函数
from bson import json_util
from kombu.utils import json

# 时区
cst_tz = timezone('Asia/Shanghai')

year = datetime.datetime.now().year
month = datetime.datetime.now().month
day = datetime.datetime.now().day
hour = datetime.datetime.now().hour
minute = datetime.datetime.now().minute
second = datetime.datetime.now().second
now_timestamp = int(time.time())
today_start_time_timestamp = int(time.mktime(time.strptime(str(datetime.date.today()), '%Y-%m-%d')))

# 数据库
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

col_ChangeLog = db.ChangeLog
col_ProductTeamtour = db.ProductTeamtour
col_ProductPlanPrice = db.ProductPlanPrice
col_AliFliggyStores = db.AliFliggyStores

const_sessionkey_main = '61011280e993b243f58b5045c012281c177417103e4b694711378241'
const_appkey_main = '27761322'
const_secret_main = '98e36ac9fa56d9c5af04a6777a67d7af'


def get_now_iso_date():
    year = datetime.datetime.now().year
    month = datetime.datetime.now().month
    day = datetime.datetime.now().day
    hour = datetime.datetime.now().hour
    minute = datetime.datetime.now().minute
    second = datetime.datetime.now().second

    return cst_tz.localize(datetime.datetime(year, month, day, hour, minute, second))

def ConsumerCallback(channel, method, properties, body):
    print((datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'), " Received: ", body)
    data = json.loads(body)
    xml = data['XmlResult']
    # body = "<?xml version='1.0' encoding='UTF-8'?><TCUpdateProductPriceRequest><RequestHeader><VendorToken>4929239f9ca5443895f52ee116123456</VendorToken><VendorId>98001</VendorId></RequestHeader><VendorProductCode>xjpzyx09180958</VendorProductCode><VendorSkuCode>sh5t4wzyx0917</VendorSkuCode><PriceList><TCPrice><StartDate>2019-09-28</StartDate><EndDate>2019-11-22</EndDate><DayOfWeek>1234567</DayOfWeek><PackagePriceInfoList><PackagePriceInfo><AdultCostPrice>2639</AdultCostPrice><ChildCostPrice>1547</ChildCostPrice><SinglePersonCostPrice>939</SinglePersonCostPrice></PackagePriceInfo></PackagePriceInfoList></TCPrice></PriceList></TCUpdateProductPriceRequest>"
    process(xml)
    # print("kevin mark here")
    # exit()

def Main():
    environment = 'test'

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
        channel.basic_consume('kevin_test', ConsumerCallback,  True)
    else:
        credentials = pika.PlainCredentials("iflying", "mq_iflying_2019")
        parameters = pika.ConnectionParameters(host="121.199.39.2",
                                               virtual_host='/',
                                               credentials=credentials)
        connection = pika.BlockingConnection(parameters)  # 连接 RabbitMQ

        channel = connection.channel()  # 创建频道

        queue = channel.queue_declare(queue='Platform.Price', durable=True)  # 声明或创建队列

        # no_ack=True 开启自动确认，不然消费后的消息会一直留在队列里面
        # no_ack = no_manual_ack = auto_ack；不手动应答，开启自动应答模式
        channel.basic_consume('Platform.Price', ConsumerCallback, True)

    print('Wait Message ...')

    channel.start_consuming()

def process(xml):
    # xml = "<?xml version='1.0' encoding='UTF-8'?><TCUpdateProductPriceRequest><RequestHeader><VendorToken>4929239f9ca5443895f52ee116123456</VendorToken><VendorId>98001</VendorId></RequestHeader><VendorProductCode>xjpzyx09180958</VendorProductCode><VendorSkuCode>sh5t4wzyx0917</VendorSkuCode><PriceList><TCPrice><StartDate>2019-09-28</StartDate><EndDate>2019-11-22</EndDate><DayOfWeek>1234567</DayOfWeek><PackagePriceInfoList><PackagePriceInfo><AdultCostPrice>2639</AdultCostPrice><ChildCostPrice>1547</ChildCostPrice><SinglePersonCostPrice>939</SinglePersonCostPrice></PackagePriceInfo></PackagePriceInfoList></TCPrice></PriceList></TCUpdateProductPriceRequest>"
    request_sku = xmltodict.parse(xml)

    if 'TCUpdateProductPriceRequest' not in request_sku:
        log = {
            'Function': 'process',
            'Subtype': 1,
            'Message': 'TCUpdateProductPriceRequest 字段不存在',
            'Request': xml,
        }
        add_log(log)
        return

    if 'VendorProductCode' not in request_sku['TCUpdateProductPriceRequest']:
        log = {
            'Function': 'process',
            'Subtype': 2,
            'Message': 'VendorProductCode 字段不存在',
            'Request': xml,
        }
        add_log(log)
        return

    if 'PriceList' not in request_sku['TCUpdateProductPriceRequest']:
        log = {
            'Function': 'process',
            'Subtype': 3,
            'Message': 'PriceList 字段不存在',
            'Request': xml,
        }
        add_log(log)
        return

    price_list = request_sku['TCUpdateProductPriceRequest']['PriceList']
    out_id = request_sku['TCUpdateProductPriceRequest']['VendorProductCode']
    VendorSkuCode = request_sku['TCUpdateProductPriceRequest']['VendorSkuCode']
    temp_res = find_itemid_by_outid(out_id)

    try:
        StartDate = str(price_list['TCPrice']['StartDate'])
        StartDate_timestamp = int(time.mktime(time.strptime(StartDate, "%Y-%m-%d")))

        if StartDate_timestamp < today_start_time_timestamp:
            log = {
                'Function': 'process',
                'Subtype': 12,
                'Message': '日期已过期',
                'Request': xml,
                'Data': temp_res,
            }
            add_log(log)
            return
    except BaseException as err:
        log = {
            'Function': 'process',
            'Subtype': 11,
            'Message': err,
            'Request': xml,
            'Data': temp_res,
        }
        add_log(log)
        return

    if not temp_res:
        log = {
            'Function': 'process',
            'Subtype': 4,
            'Message': 'find_itemid_by_outid 失败',
            'Request': xml,
        }
        add_log(log)
        return

    try:
        item_id = str(temp_res['AliField']['item_id'])
    except BaseException as err:
        log = {
            'Function': 'process',
            'Subtype': 5,
            'Message': 'item_id 赋值失败',
            'Request': xml,
            'Data': temp_res,
        }
        add_log(log)
        return

    if not item_id:
        log = {
            'Function': 'process',
            'Subtype': 6,
            'Message': 'item_id 为空',
            'Request': xml,
            'Data': temp_res,
        }
        add_log(log)
        return

    skus = set_skus(item_id, price_list, VendorSkuCode)

    if not skus:
        log = {
            'Function': 'process',
            'Subtype': 8,
            'Message': 'set_skus 失败',
            'Request': xml,
        }
        add_log(log)
        return

    override_res = taobao_alitrip_travel_item_sku_override(item_id, skus)

    override_success = False
    try:
        if override_res['alitrip_travel_item_sku_override_response']['travel_item']['item_id']:
            override_success = True
    except BaseException as err:
        print(err)

    if override_success:
        final_msg = '修改价格成功' + ' result: ', override_res
    else:
        final_msg = '修改价格失败' + ' result: ', override_res

    log = {
        'Function': 'process',
        'Subtype': 9,
        'Message': final_msg,
        'Data': override_res,
    }
    add_log(log)

# 设置请求用的skus字段
def set_skus(item_id, price_list, VendorSkuCode):
    temp_res = single_query(item_id, 0)
    return_sku = []

    try:
        TCPrice = price_list['TCPrice']

        sku_infos = temp_res['alitrip_travel_item_single_query_response']['travel_item']['sku_infos']['pontus_travel_item_sku_info']

        if 'outer_sku_id' in sku_infos:
            sku_infos = [
                sku_infos
            ]

        for sku in sku_infos:
            temp_packge = {
                'outer_sku_id': '',
                'package_name': sku['package_name'],
                'prices': sku['prices']['pontus_travel_prices'],
            }

            if 'outer_sku_id' in sku:
                temp_packge['outer_sku_id'] = sku['outer_sku_id']

            if 'date' in temp_packge['prices']:
                temp_packge['prices'] = [
                    temp_packge['prices']
                ]

            new_packge_prices = []
            for prices in temp_packge['prices']:
                date = str(prices['date'])
                date = int(time.mktime(time.strptime(date, "%Y-%m-%d %H:%M:%S")))

                if date < today_start_time_timestamp:
                    continue

                if temp_packge['outer_sku_id'] == VendorSkuCode and prices['date'].split(' ')[0] == TCPrice['StartDate'] \
                        and (int(prices['price_type']) == 1 or int(prices['price_type']) == 3):

                    if int(prices['price_type']) == 1:
                        prices['price'] = int(TCPrice['PackagePriceInfoList']['PackagePriceInfo']['AdultCostPrice'])*100

                    if int(prices['price_type']) == 3:
                        prices['price'] = int(TCPrice['PackagePriceInfoList']['PackagePriceInfo']['SinglePersonCostPrice'])*100

                    try:
                        op_data = {}

                        if int(prices['price_type']) == 1:
                            op_data['DefaultPrice'] = float(prices['price'] / 100)

                        if int(prices['price_type']) == 2:
                            op_data['ChildPrice'] = float(prices['price'] / 100)

                        if int(prices['price_type']) == 3:
                            op_data['SingleRoomDiff'] = float(prices['price'] / 100)
                    except BaseException as err:
                        pass

                    modify_price_in_erp(item_id, temp_packge['outer_sku_id'], prices, op_data)
                # elif temp_packge['outer_sku_id'] == VendorSkuCode and prices['date'].split(' ')[0] == TCPrice['StartDate'] and prices['price_type'] == 2:
                #     prices['price'] = int(TCPrice['PackagePriceInfoList']['PackagePriceInfo']['ChildCostPrice'])*100

                new_packge_prices.append(prices)

            if new_packge_prices:
                temp_packge['prices'] = new_packge_prices
                return_sku.append(temp_packge)

        return return_sku
    except BaseException as err:
        log = {
            'Function': 'process',
            'Subtype': 7,
            'Message': 'set_skus 异常: ' + str(err),
            'Data': temp_res,
        }
        add_log(log)
        return False


def modify_price_in_erp(item_id, outer_sku_id, price, op_data):
    """
    修改erp中的价格方案的价格
    """
    temp_condition = {
        'AliField.item_id': str(item_id),
        'AliField.outer_sku_id': str(outer_sku_id),
        'AliField.date': str(price['date']),
        'AliField.price_type': '1',
    }

    if 'DefaultPrice' in op_data:
        op_data['AliField.price'] = str(price['price'])

    col_ProductPlanPrice.update_one(
        temp_condition,
        {
            "$set": op_data
        },
    )


# 查询详情
def single_query(num_iid, times):
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
            print(err)

    except BaseException as err:
        print('Exception single_query: ', err)

        if times < 10:
            times += 1

            time.sleep(1)
            print('第', times, '次重试single_query')
            single_query(num_iid, times)


# 添加日志
def add_log(data):
    msg = (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'), ': ', data['Message']
    print(msg)

    log_file_name = 'fliggy_log.txt'
    with open(log_file_name, 'a') as file_obj:
        file_obj.write(str(msg) + " \n")

    insert_data = {
        'userID': ObjectId('5d8c5c2228142658548b456a'),
        'userName': '系统管理员Online',
        'departmentID': ObjectId('000000000000000000000781'),
        'departmentName': '浙江恒越信息科技有限公司',
        'time': get_now_iso_date(),
        'type': 26,
        'data': {
            'Language': 'python3',
            'Class': 'mq_vision_receive_price_modify',
        },
    }

    insert_data['data'].update(data)

    col_ChangeLog.insert_one(insert_data)



def find_itemid_by_outid(out_id):
    condition = {
        'AliField.base_info.out_id': str(out_id),
        'IsDel': 0,
    }
    
    project = {
        'AliField.item_id': 1,
    }

    return col_ProductTeamtour.find_one(condition, project)


#【API3.0】商品级别日历价格库存修改，全量覆盖
# https://open.taobao.com/api.htm?docId=25759&docType=2
def taobao_alitrip_travel_item_sku_override(item_id, skus):
    # -*- coding: utf-8 -*-

    req = top.api.AlitripTravelItemSkuOverrideRequest('gw.api.taobao.com', 80)
    req.set_app_info(top.appinfo(const_appkey_main, const_secret_main))

    req.item_id = item_id
    # req.skus = [{"outer_sku_id":"sh6t5wzyx0917","package_name":"上海6天5晚自由行","prices":[{"date":"2019-10-02 00:00:00","price":"999900","price_type":"1","stock":"2"},{"date":"2019-10-07 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-12 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-17 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-11 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-01 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-06 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-20 00:00:00","price":"889800","price_type":"1","stock":"1"},{"date":"2019-10-05 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-10 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-15 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-04 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-09 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-14 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-03 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-08 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-13 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-18 00:00:00","price":"999900","price_type":"1","stock":"1"},{"date":"2019-10-02 00:00:00","price":"999900","price_type":"2","stock":"2"},{"date":"2019-10-07 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-12 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-17 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-11 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-01 00:00:00","price":"999900","price_type":"2","stock":"2"},{"date":"2019-10-06 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-20 00:00:00","price":"888400","price_type":"2","stock":"3"},{"date":"2019-10-05 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-10 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-15 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-04 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-09 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-14 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-03 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-08 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-13 00:00:00","price":"999900","price_type":"2","stock":"1"},{"date":"2019-10-18 00:00:00","price":"999900","price_type":"2","stock":"1"}]},{"outer_sku_id":"hznb6t5wzyx0917","package_name":"杭州宁波6天5晚自由行","prices":{"date":"2019-10-26 00:00:00","price":"999900","price_type":"1","stock":"1"}},{"outer_sku_id":"nb5t4wzyx0917","package_name":"宁波5天4晚自由行","prices":{"date":"2019-10-27 00:00:00","price":"444400","price_type":"1","stock":"44"}},{"outer_sku_id":"hz5t4wzyx","package_name":"杭州5天4晚自由行","prices":{"date":"2019-10-28 00:00:00","price":"777700","price_type":"1","stock":"7"}},{"outer_sku_id":"sh5t4wzyx0917","package_name":"上海5天4晚自由行","prices":{"date":"2019-10-28 00:00:00","price":"888800","price_type":"1","stock":"0"}}]
    req.skus = skus

    try:
        resp = req.getResponse(const_sessionkey_main)
        return resp
    except BaseException as err:
        print(err)
        return False

def init_fliggy_param():
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
            'Subtype': 15,
            'Message': '获取飞猪店铺数据失败',
            'Data': {
                'Result': res,
            },
        }
        add_log(log)
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


if __name__ == '__main__':
    pid = str(os.getpid())
    pid_file = "mq_price_modify.pid"

    if os.path.isfile(pid_file):
        print("%s already exists, exiting" % pid_file)
        sys.exit()

    f = open(pid_file, 'w')
    f.write(pid)
    f.close()

    try:
        try:
            init_fliggy_param()
            Main()
        except BaseException as err:
            print('Exception: ', err)
            log = {
                'Function': '__main__',
                'Subtype': 10,
                'Message': str(err),
            }
            add_log(log)
    finally:
        os.unlink(pid_file)
