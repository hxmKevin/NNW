# encoding: utf-8
# author: kevin
# date: 2019-10
# summary: 接收方/消费者

import os, sys, time
from dateutil import parser
import pika
import json
import requests
from pymongo import ReturnDocument, InsertOne, UpdateOne
from common.readconfig import ReadConfig
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

col_ChangeLog = db.FliggyLog
col_ProductTeamtour = db.ProductTeamtour
col_ProductPlanPrice = db.ProductPlanPrice
col_AliFliggyStores = db.AliFliggyStores
col_ids = db.ids
col_ProductPlan = db.ProductPlan

const_sessionkey_main = '61011280e993b243f58b5045c012281c177417103e4b694711378241'
const_appkey_main = '27761322'
const_secret_main = '98e36ac9fa56d9c5af04a6777a67d7af'

const_independent_travel_businessTypeID = '5c061966c98f8128008b4574'
const_independent_travel_businessType = {
    'id': 2,
    'name': '自由行',
    'sub_type': {
        'id': 1,
        'name': '机+酒',
    },
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
const_AddInfo = {
    'ForeignKeyID': ObjectId('000000000000000000002251'),
    'EmployeeName': '系统管理员Online',
    'EmployeeDepartmentID': ObjectId('000000000000000000000781'),
    'EmployeeDepartmentName': '浙江恒越信息科技有限公司',
}
# newest Subtype 20


def get_now_iso_date():
    year = datetime.datetime.now().year
    month = datetime.datetime.now().month
    day = datetime.datetime.now().day
    hour = datetime.datetime.now().hour
    minute = datetime.datetime.now().minute
    second = datetime.datetime.now().second

    return cst_tz.localize(datetime.datetime(year, month, day, hour, minute, second))


def ConsumerCallback(channel, method, properties, body):
    # body = '{"VendorProductCode":"598145720467","VendorSkuCode":"HXHKG190300003","PriceList":[{"StartDate":"2020-11-16 00:00:00","AdultCostPrice":7636.92,"SinglePersonCostPrice":1307.0,"ReservedInventoryQuantity":1110},{"StartDate":"2019-11-16 00:00:00","AdultCostPrice":7789.96,"SinglePersonCostPrice":1332.0,"ReservedInventoryQuantity":1499},{"StartDate":"2019-11-17 00:00:00","AdultCostPrice":2715.23,"SinglePersonCostPrice":815.0,"ReservedInventoryQuantity":837},{"StartDate":"2019-11-18 00:00:00","AdultCostPrice":1309.35,"SinglePersonCostPrice":603.0,"ReservedInventoryQuantity":243},{"StartDate":"2019-11-19 00:00:00","AdultCostPrice":6178.6,"SinglePersonCostPrice":798.0,"ReservedInventoryQuantity":890}],"QueueConfig":"Platform.NewPlans|1|2"}'
    print((datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'), " Received: ", body)
    data = json.loads(body)

    if 'VendorProductCode' not in data or 'VendorSkuCode' not in data or 'PriceList' not in data:
        log = {
            'Function': 'ConsumerCallback',
            'Subtype': 16,
            'Message': '信息结构不合法',
            'Request': body,
        }
        add_log(log)
        return

    if not data['PriceList']:
        log = {
            'Function': 'ConsumerCallback',
            'Subtype': 17,
            'Message': 'PriceList 为空, 无需执行',
            'Request': body,
        }
        add_log(log)
        return

    process(data)


def Main():
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
        channel.basic_consume('kevin_test', ConsumerCallback,  True)
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

        queue = channel.queue_declare(queue='Platform.NewPlans', durable=True)  # 声明或创建队列

        # no_ack=True 开启自动确认，不然消费后的消息会一直留在队列里面
        # no_ack = no_manual_ack = auto_ack；不手动应答，开启自动应答模式
        channel.basic_consume('Platform.NewPlans', ConsumerCallback, True)

    print('Wait Message ...')

    channel.start_consuming()


def get_product_one(item_id, outer_sku_id):
    temp_condition = {
        'IsDel': 0,
        'AliField.item_id': str(item_id),
        'AliField.sku_info.outer_sku_id': str(outer_sku_id),
    }
    temp_project = {
        'Belong': 1,
        'ProductNo': 1,
        'ProductGADDR': 1,
        'Title': 1,
        'WebTitle': 1,
        'AliField.base_info.trip_max_days': 1,
        'AliField.base_info.accom_nights': 1,
        'AliField.sku_info.outer_sku_id': 1,
        'AliField.sku_info.package_name': 1,
        'AliField.sku_info.package_desc': 1,
    }

    return col_ProductTeamtour.find_one(temp_condition, temp_project)


def process(data):
    item_id = str(data['VendorProductCode'])
    outer_sku_id = str(data['VendorSkuCode'])

    product_data = get_product_one(item_id, outer_sku_id)
    if not product_data:
        log = {
            'Function': 'ConsumerCallback',
            'Subtype': 19,
            'Message': 'erp 不存在业务相关产品: item_id ' + item_id + ', 无法继续执行',
            'Request': data,
        }
        add_log(log)
        return

    pre_plan_list = get_pre_plan_list(item_id, outer_sku_id)

    ns_plan = []
    ns_price = []
    for price in data['PriceList']:
        if 'StartDate' not in price or 'AdultCostPrice' not in price or 'SinglePersonCostPrice' not in price or 'ReservedInventoryQuantity' not in price:
            log = {
                'Function': 'process',
                'Subtype': 18,
                'Message': 'PriceList 字段结构不完整',
                'Request': data,
            }
            add_log(log)
            continue

        # 封装计划表新建数据
        op_data_plan = set_plan_op_data(product_data, price, item_id, outer_sku_id)
        if str(price['StartDate']) in pre_plan_list:
            op_data_plan['UpdateTime'] = op_data_plan.pop('AddTime')
            op_data_plan.pop('_id')

        # 组建计划语句
        temp_condition = {
            'IsDel': 0,
            'fliggy_item_id': str(item_id),
            'fliggy_outer_sku_id': str(outer_sku_id),
            'fliggy_date': str(price['StartDate']),
        }
        operation = UpdateOne(
            temp_condition,
            {'$set': op_data_plan},
            True,
        )
        ns_plan.append(operation)

        # 组建价格方案语句
        op_data_price = set_price_op_data(op_data_plan, item_id, price)
        if str(price['StartDate']) in pre_plan_list:
            op_data_price['UpdateTime'] = op_data_price.pop('AddTime')
            op_data_price['PlanID'] = pre_plan_list[str(price['StartDate'])]['_id']
        temp_condition = {
            'IsDel': 0,
            'fliggy_item_id': str(item_id),
            'fliggy_outer_sku_id': str(outer_sku_id),
            'fliggy_date': str(price['StartDate']),
        }
        operation = UpdateOne(
            temp_condition,
            {'$set': op_data_price},
            True,
        )
        ns_price.append(operation)

    if len(ns_plan) > 0:
        col_ProductPlan.bulk_write(ns_plan)

    if len(ns_price) > 0:
        col_ProductPlanPrice.bulk_write(ns_price)

    log = {
        'Function': 'process',
        'Subtype': 20,
        'Message': '团期新建成功',
        'Data': data,
    }
    add_log(log)


def set_price_op_data(plan_data, item_id, price):
    """
    封装价格方案表新建数据
    """
    op_data = {}
    op_data['ProductType'] = const_product_type
    op_data['SubTypeSource'] = 1
    if '_id' in plan_data:
        op_data['PlanID'] = ObjectId(plan_data['_id'])
    op_data['ProductID'] = ObjectId(plan_data['ProductID'])
    op_data['PlanNumber'] = plan_data['PlanNumber']
    op_data['MorethanNumber'] = plan_data['MorethanNumber']
    op_data['DefaultPrice'] = float(price['AdultCostPrice'])
    op_data['SingleRoomDiff'] = float(price['SinglePersonCostPrice'])
    op_data['ChildPrice'] = 0.0
    op_data['BabyPrice'] = 0.0
    op_data['FliggyPriceType'] = int(plan_data['fliggy_price_type'])
    op_data['fliggy_item_id'] = str(item_id)
    op_data['fliggy_outer_sku_id'] = str(plan_data['fliggy_outer_sku_id'])
    op_data['fliggy_date'] = str(plan_data['fliggy_date'])
    op_data['fliggy_price_type'] = str(plan_data['fliggy_price_type'])
    op_data['fliggy_stock'] = str(plan_data['fliggy_stock'])
    op_data['fliggy_price'] = str(plan_data['fliggy_price'])
    op_data['AddInfo'] = const_AddInfo
    op_data['AddTime'] = get_now_iso_date()
    op_data['ID'] = 0
    op_data['IsDel'] = 0
    op_data['isDefault'] = 0
    op_data['RetainNumber'] = 0
    op_data['PersistenceNumber'] = 0
    op_data['PaidNumber'] = 0
    op_data['IsDeductiblevoucher'] = 0
    op_data['Deductiblevoucher'] = 0

    price_type = ''
    try:
        if int(plan_data['fliggy_price_type']) == 1:
            price_type = '成人'

        if int(plan_data['fliggy_price_type']) == 2:
            price_type = '小孩'

        if int(plan_data['fliggy_price_type']) == 3:
            price_type = '单房差'
    except BaseException as err:
        pass
    op_data['Pricetitle'] = str(plan_data['fliggy_package_name'] + '-' + str(price_type))
    if 'fliggy_package_desc' in plan_data:
        op_data['Remark'] = str(plan_data['fliggy_package_desc'])
    else:
        op_data['Remark'] = None

    return op_data


def set_plan_op_data(product_data, price, item_id, outer_sku_id):
    """
    封装计划表新建数据
    """
    date_str = str(datetime.datetime.strptime(price['StartDate'], "%Y-%m-%d %H:%M:%S"))
    date_timestamp = int(time.mktime(time.strptime(date_str, "%Y-%m-%d %H:%M:%S")))

    try:
        trip_max_days = int(product_data['AliField']['base_info']['trip_max_days'])
        accom_nights = int(product_data['AliField']['base_info']['accom_nights'])
    except BaseException as err:
        trip_max_days = 1
        accom_nights = 1

    end_timestamp = date_timestamp + 86400 * trip_max_days
    end_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end_timestamp))

    op_data = {}
    op_data['_id'] = ObjectId()
    op_data['IsDel'] = 0
    op_data['ProductNo'] = str(product_data['ProductNo'])
    op_data['ProductID'] = ObjectId(product_data['_id'])
    op_data['ProductName'] = str(product_data['Title'])
    if 'WebTitle' in product_data:
        op_data['FliggyProductTitle'] = str(product_data['WebTitle'])
    op_data['PlanNo'] = mid('ProductPlan', 8, 'P')
    op_data['fliggy_item_id'] = str(item_id)
    op_data['fliggy_outer_sku_id'] = str(outer_sku_id)
    op_data['fliggy_package_name'] = str(product_data['AliField']['sku_info']['package_name'])
    if 'package_desc' in product_data['AliField']['sku_info']:
        op_data['fliggy_package_desc'] = str(product_data['AliField']['sku_info']['package_desc'])
    op_data['fliggy_date'] = str(price['StartDate'])
    op_data['fliggy_price'] = str(price['AdultCostPrice'])
    op_data['fliggy_price_type'] = '1'
    op_data['fliggy_stock'] = str(price['ReservedInventoryQuantity'])

    op_data['StartTime'] = cst_tz.localize(parser.parse(date_str))
    op_data['EndTime'] = cst_tz.localize(parser.parse(end_str))
    op_data['ProductGADDR'] = product_data['ProductGADDR']
    op_data['TravelDays'] = trip_max_days
    op_data['LateNight'] = accom_nights
    op_data['PlanNumber'] = price['ReservedInventoryQuantity']
    op_data['MorethanNumber'] = price['ReservedInventoryQuantity']
    op_data['business_type_id'] = ObjectId(const_independent_travel_businessTypeID)
    op_data['business_type'] = const_independent_travel_businessType
    op_data['CompanyInfo'] = const_CompanyInfo
    op_data['ProductType'] = const_product_type
    op_data['SubTypeSource'] = 1
    op_data['Belong'] = product_data['Belong']
    op_data['AddTime'] = get_now_iso_date()
    op_data['AddInfo'] = const_AddInfo
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
    op_data['CreateSource'] = 'mq_vision_new_date'

    return op_data


def get_pre_plan_list(item_id, outer_sku_id):
    temp_condition = {
        'IsDel': 0,
        'fliggy_item_id': str(item_id),
        'fliggy_outer_sku_id': str(outer_sku_id),
    }
    temp_project = {
        'fliggy_date': 1,
    }
    plan_list = col_ProductPlan.find(temp_condition, temp_project)

    pre_plan_list = {}
    for plan in plan_list:
        pre_plan_list[str(plan['fliggy_date'])] = plan

    return pre_plan_list

# 添加日志
def add_log(data):
    msg = (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'), ': ', data['Message']
    print(msg)

    log_file_name = 'mq_vision_new_date_log.txt'
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
            'Class': 'mq_vision_new_date',
        },
    }

    insert_data['data'].update(data)

    col_ChangeLog.insert_one(insert_data)



def mid(collection, digit, condition):
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

