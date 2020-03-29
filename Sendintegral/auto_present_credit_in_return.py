"""""
@name           自动发放前一天回团订单积分
@version        1.32 in 2019.9.30
@edit_log       
    2019.8.13   http://git.iflying.com/Business/ERP/erp/issues/238
    2019.9.16   1.3     http://git.iflying.com/Business/ERP/erp/issues/269
    2019.9.25   1.31     下单时间10.1之后的按1.3版本发积分,之前按1.2版本发积分
    2019.9.30   1.32     下单时间10.1之后的按1.32版本发积分(相对于1.2版本有邀请人制度),之前按1.2版本发积分
    2019.10.31  1.4     加入 提醒私顾给客户打标签 功能
    2019.11.26  1.5     给新会员体系发积分 http://git.iflying.com/Business/ERP/erp/issues/314
    2019.12.24  1.6     http://git.iflying.com/Business/ERP/members/issues/13
"""""
# coding=UTF-8
# !/usr/bin/python3
import os
import sys
import pymongo
import datetime
import time
import socket
import requests
from bson.objectid import ObjectId
from pytz import utc
from pytz import timezone
from bson import json_util
import json
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from common.readconfig import ReadConfig

# 时区
cst_tz = timezone('Asia/Shanghai')

# 所需常量
today = datetime.date.today()
today_time = int(time.mktime(today.timetuple()))
today = time.strftime('%Y-%m-%d', time.localtime(time.time()))

year = datetime.datetime.now().year
month = datetime.datetime.now().month
day = datetime.datetime.now().day
hour = datetime.datetime.now().hour
minute = datetime.datetime.now().minute
second = datetime.datetime.now().second

year_yesterday = (datetime.date.today() + datetime.timedelta(-1)).year
month_yesterday = (datetime.date.today() + datetime.timedelta(-1)).month
day_yesterday = (datetime.date.today() + datetime.timedelta(-1)).day

# now_iso_date = datetime.datetime(year, month, day, hour, minute, second).replace(tzinfo=cst_tz)
now_iso_date = cst_tz.localize(datetime.datetime(year, month, day, hour, minute, second))

timestamp_1001 = 1569859200

# 提示
msg_customer_data_not_found = '会员查无数据'
msg_group_id_not_exists = '不存在group_id字段'
msg_not_exists = '不存在'
msg_group_id_not_valid = 'group_id数值不符合'
msg_unknown_error = '未知错误'
msg_eligibility = '符合条件'
msg_eligibility_new = '符合条件-新会员积分'
msg_credit_zero = '实发积分为0'
msg_present_already = '积分已赠送'
# group_id==2是老班长；group_id==3是翰林

# 黑卡+钻石卡
constant_black_diamond_card = ['5ddb47a6bba63907008b4578', '5ddb47bebba63906008b4578']
# 金卡+银卡
constant_gold_silver_card = ['5ddb478abba63908008b4576', '5ddb467ebba63906008b4577']


def get_host_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 80))
    ip = s.getsockname()[0]

    return ip


def check_present_credit(customer_id, order_id):
    condition = {
        'DelStatus': 0,
        'CustomerID': ObjectId(customer_id),
        'OrderID': ObjectId(order_id),
        'IntegralSourceTypeID': 1,
    }

    project = {
        '_id': 1,
    }

    find_log = col_customer_integral_record.find_one(condition, project)

    if find_log:
        return 1

    condition = {
        'DelStatus': 0,
        'CustomerID': ObjectId(customer_id),
        'OrderID': ObjectId(order_id),
        'IntegralSourceTypeID': 1,
    }

    project = {
        '_id': 1,
    }

    find_log = col_customer_level_integral_record.find_one(condition, project)

    if find_log:
        return 1

    return 2


def deal_other_tourists(main_customer_id, order_data, tourists_issued_credit):
    condition = {
        'DelStatus': 0,
        'TouristStatus': 1,
        'OrderID': ObjectId(order_data['_id']),
    }
    order_tourists_list = col_order_tourists.find(condition)

    for order_tourist in order_tourists_list:
        temp_condition_or = []

        try:
            if order_tourist['CertificatesType']['ForeignKeyID'] == ObjectId('000000000000000000000001') and len(order_tourist['TouristDocumentNumber']) > 0:
                temp_condition_or.append({'Enlarge.CustomerDocumentNumber': order_tourist['TouristDocumentNumber']})
        except BaseException as err:
            pass

        if not temp_condition_or:
            try:
                if len(order_tourist['TouristMobile']) > 0 and len(order_tourist['TouristName']) > 0:
                    temp_condition_or.append({'CustomerMobile': order_tourist['TouristMobile'], 'CustomerName': order_tourist['TouristName']})
            except BaseException as err:
                pass

        if temp_condition_or:
            temp_condition = {
                'DelStatus': 0,
                '$or': [
                    {'Company.ForeignKeyID': ObjectId('000000000000000000000001')},
                    {'Company.ForeignKeyID': ObjectId('000000000000000000000028')},
                ],
                '$and': [
                    {
                        '$or': temp_condition_or,
                    },
                ],
            }
            temp_customer_data = col_customers.find(temp_condition)
            temp_customer_data_count = col_customers.count_documents(temp_condition)

            if temp_customer_data_count > 0:
                if temp_customer_data_count == 1:
                    for data in temp_customer_data:
                        customer_data = data
                        break
                else:
                    customer_data = deal_multi_customer(temp_customer_data)

                if customer_data['_id'] != ObjectId(main_customer_id):
                    tourist_id = order_tourist['_id']
                    tourists_issued_credit = main_judge(customer_data['_id'], order_data, 'other', tourists_issued_credit, customer_data, tourist_id)

    return tourists_issued_credit


# 游客的身份证对应 customers 表中多个documents 从中选取合适的 document 作为客户信息
# 如果只有一条数据有 group_lv = 1 则返回该条数据
# 如果只有一条数据有 group_id 则返回该条数据
# 不满足上述情况范围较早的一条数据


def deal_multi_customer(customer_data_list):
    list = {}
    list['group_id_exists_2_3'] = []
    list['group_lv_exists_1'] = []
    i = 0

    for customer_data in customer_data_list:
        i += 1
        if i == 1:
            list['old'] = customer_data
        if "group_id" in customer_data:
            if customer_data['group_id'] == 2 or customer_data['group_id'] == 3:
                list['group_id_exists_2_3'].append(customer_data)

        if "group_lv" in customer_data:
            if customer_data['group_lv'] == 1:
                list['group_lv_exists_1'].append(customer_data)

    if list['group_lv_exists_1']:
        return list['group_lv_exists_1'].pop(0)
    elif list['group_id_exists_2_3']:
        return list['group_id_exists_2_3'].pop(0)

    return list['old']


def deal_inventor(customer_data, order_data, temp_credit):
    """
    name    处理邀请人发放积分
    """
    inventor_data = col_customers.find_one({'_id': customer_data['InviterID']})

    op_data = {
        "CustomerType": 'inventor',
        "DelStatus": 0,
        "CustomerID": customer_data['InviterID'],
        "OrderID": ObjectId(order_data['_id']),
    }

    if not inventor_data:
        op_data['Sent'] = 0
        op_data['Remark'] = msg_customer_data_not_found
        op_data['Status'] = 2
    elif "group_id" not in inventor_data:
        op_data['Sent'] = 0
        op_data['Remark'] = 'group_id' + msg_not_exists
        op_data['Status'] = 3
    elif inventor_data['group_id'] != 2:
        op_data['Sent'] = 0
        op_data['Remark'] = msg_group_id_not_valid
        op_data['Status'] = 4
        op_data['group_id'] = inventor_data['group_id']
    else:
        if "CustomerName" in inventor_data:
            op_data['CustomerName'] = inventor_data['CustomerName']

        op_data['Sent'] = 1
        op_data['Remark'] = msg_eligibility
        op_data['Status'] = 1
        op_data['group_id'] = inventor_data['group_id']
        op_data['CustomerID'] = inventor_data['_id']
        op_data['InviteeID'] = customer_data['_id']
        op_data['Credit'] = temp_credit
        op_data['OrderData'] = order_data
        op_data['CustomerData'] = inventor_data
        op_data['InviteeData'] = customer_data

        do_present(order_data, 'inventor', op_data)

    insert_auto_present_credit_log(op_data)
    print('OrderID : ', order_data['_id'], 'main_judge结果 : ', op_data['Remark'], 'customer_type:',
          'inventor')


def do_present(order_data, customer_type, op_data):
    insert_data = {
        'CreateTime': now_iso_date,
        'CreateUserID': ObjectId(op_user_id),
        'CreateUserIP': get_host_ip(),
        'DelStatus': 0,
        'CustomerID': op_data['CustomerID'],
        'OrderID': ObjectId(order_data['_id']),
        'IntegralSourceTypeID': 1,
        'IntegralValue': op_data['Credit'],
        'IntegralOperationNotes': '订单' + order_data['OrderNo'] + ':' + order_data['ProductTitle'] + '[' + time.strftime(
            '%Y-%m-%d', time.localtime(order_data['TeamStartTime'].timestamp())) + ']出发.',
        'CustomerType': customer_type,
    }

    if customer_type == "inventor":
        insert_data['InviteeOrderID'] = insert_data.pop('OrderID')
        insert_data['IntegralOperationNotes'] = '作为邀请人获得积分: ' + insert_data['IntegralOperationNotes']


    # 更新会员积分
    col_customers.update_one(
        {
            '_id': insert_data['CustomerID']
        },
        {
            "$set": {"UpdateTime": now_iso_date},
            "$inc": {"CustomerIntegral": insert_data['IntegralValue']}
        }
    )

    # 更新订单发放积分状态
    col_orders.update_one(
        {
            '_id': ObjectId(order_data['_id'])
        },
        {
            "$set": {"CustomerEnlarge.IntegralStatus": 1}
        },
    )

    # 添加积分发放记录
    col_customer_integral_record.insert_one(insert_data)

    print('OrderID : ', order_data['_id'], '发放', op_data['Credit'], '积分, customer_type :', customer_type,
          'customer_id:', op_data['CustomerID'])


def do_present_new(order_data, customer_type, op_data):
    """
    发放新会员积分
    """
    insert_data = {
        'CreateTime': now_iso_date,
        'CreateUserID': ObjectId(op_user_id),
        'CreateUserIP': get_host_ip(),
        'DelStatus': 0,
        'CustomerID': op_data['CustomerID'],
        'OrderID': ObjectId(order_data['_id']),
        'IntegralSourceTypeID': 1,
        'IntegralValue': op_data['Credit'],
        'IntegralOperationNotes': '订单' + order_data['OrderNo'] + ':' + order_data['ProductTitle'] + '[' + time.strftime(
            '%Y-%m-%d', time.localtime(order_data['TeamStartTime'].timestamp())) + ']出发.',
        'CustomerType': customer_type,
    }

    # 更新会员积分
    col_customers.update_one(
        {
            '_id': insert_data['CustomerID']
        },
        {
            "$set": {"UpdateTime": now_iso_date},
            "$inc": {"CustomerLevelIntegral": insert_data['IntegralValue']}
        }
    )

    # 更新订单发放积分状态
    col_orders.update_one(
        {
            '_id': ObjectId(order_data['_id'])
        },
        {
            "$set": {"CustomerEnlarge.IntegralStatus": 1}
        },
    )

    # 添加积分发放记录
    col_customer_level_integral_record.insert_one(insert_data)

    print('OrderID : ', order_data['_id'], '发放', op_data['Credit'], '积分, customer_type :', customer_type, 'customer_id:', op_data['CustomerID'])


"""
name    发放积分主程序
param
    customer_id             客户_id
    order_data              订单数据 
    customer_type           客户类型    main-下单人    other-游客    format:string
    tourists_issued_credit  游客已发积分 , 影响下单人可获得积分    
    customer_data           客户数据 , 如果是空需要根据 customer_id 重新获取
"""


def main_judge(customer_id, order_data, customer_type, tourists_issued_credit, customer_data={}, tourist_id=''):
    if not customer_data:
        customer_data = col_customers.find_one({'_id': ObjectId(customer_id)})

    try:
        CustomerLevelID = str(customer_data['Enlarge']['CustomerLevelID'])
    except BaseException as err:
        CustomerLevelID = ''

    op_data = {
        "CustomerType": customer_type,
        "DelStatus": 0,
        "CustomerID": customer_id,
        "OrderID": ObjectId(order_data['_id']),
    }

    if "ProductTitle" in order_data:
        op_data['ProductTitle'] = order_data['ProductTitle']

    if "OrderNo" in order_data:
        op_data['OrderNo'] = order_data['OrderNo']

    if not customer_data:
        op_data['Sent'] = 0
        op_data['Remark'] = msg_customer_data_not_found
        op_data['Status'] = 2

    elif "TotalFinalPrice" not in order_data:
        op_data['Sent'] = 0
        op_data['Remark'] = 'TotalFinalPrice' + msg_not_exists
        op_data['Status'] = 11

    elif "TotalNumber" not in order_data:
        op_data['Sent'] = 0
        op_data['Remark'] = 'TotalNumber' + msg_not_exists
        op_data['Status'] = 12

    elif "OrderNo" not in order_data:
        op_data['Sent'] = 0
        op_data['Remark'] = 'OrderNo' + msg_not_exists
        op_data['Status'] = 13

    elif "ProductTitle" not in order_data:
        op_data['Sent'] = 0
        op_data['Remark'] = 'ProductTitle' + msg_not_exists
        op_data['Status'] = 14

    elif "TeamStartTime" not in order_data:
        op_data['Sent'] = 0
        op_data['Remark'] = 'TeamStartTime' + msg_not_exists
        op_data['Status'] = 15

    elif check_present_credit(customer_id, order_data['_id']) == 1:
        op_data['Sent'] = 0
        op_data['Remark'] = msg_present_already
        op_data['Status'] = 7
        op_data['CustomerData'] = customer_data
        op_data['OrderData'] = order_data

    elif customer_type == 'main' and (
            CustomerLevelID in constant_black_diamond_card or CustomerLevelID in constant_gold_silver_card):
        if CustomerLevelID in constant_black_diamond_card:
            op_data['Credit'] = (int(order_data['TotalFinalPrice'])) * 2
        if CustomerLevelID in constant_gold_silver_card:
            op_data['Credit'] = int(order_data['TotalFinalPrice'])

        op_data['Sent'] = 1
        op_data['Remark'] = msg_eligibility_new
        op_data['Status'] = 21
        op_data['CustomerLevelID'] = CustomerLevelID
        op_data['CustomerData'] = customer_data
        op_data['OrderData'] = order_data
        op_data['CustomerID'] = customer_id

        customer_type = 'new'
        do_present_new(order_data, customer_type, op_data)

    elif "group_id" not in customer_data:
        # 2019.8.12 恢复给游客发积分
        if customer_type == 'main':
            # tourists_issued_credit: 游客符合发放条件 已发分数
            tourists_issued_credit = deal_other_tourists(customer_data['_id'], order_data, tourists_issued_credit)

        op_data['Sent'] = 0
        op_data['Remark'] = 'group_id' + msg_not_exists
        op_data['Status'] = 3

    elif customer_data['group_id'] != 2 and customer_data['group_id'] != 3:
        # 2019.8.12 恢复给游客发积分
        if customer_type == 'main':
            # tourists_issued_credit:游客符合发放条件人数
            tourists_issued_credit = deal_other_tourists(customer_data['_id'], order_data, tourists_issued_credit)

        op_data['Sent'] = 0
        op_data['Remark'] = msg_group_id_not_valid
        op_data['Status'] = 4
        op_data['group_id'] = customer_data['group_id']

    # > 需要发放积分情况 <
    elif customer_data['group_id'] == 2 or customer_data['group_id'] == 3:
        # 2019.8.12 恢复给游客发积分

        # 下定日期时间戳, 判断是否大于10月1号时需要使用
        timestamp_CreateTime = int(time.mktime((order_data['CreateTime']).timetuple())) + 8 * 60 * 60

        # 下单人能获得积分的百分比
        if customer_type == 'main':
            # tourists_issued_credit: 游客符合发放条件 已发分数
            tourists_issued_credit = deal_other_tourists(customer_data['_id'], order_data, tourists_issued_credit)

            if "group_lv" not in customer_data:
                customer_data['group_lv'] = 0

            if customer_data['group_lv'] < 1 and tourists_issued_credit < int(order_data['TotalFinalPrice']):
                op_data['Credit'] = int(order_data['TotalFinalPrice']) - tourists_issued_credit
            else:
                op_data['Credit'] = int(order_data['TotalFinalPrice']) * 2 - tourists_issued_credit
        if customer_type == 'other':
            percent = 1 / order_data['TotalNumber']

            op_data['Credit'] = int(order_data['TotalFinalPrice'] * percent)
            if "group_lv" in customer_data:
                if customer_data['group_lv'] >= 1:
                    op_data['Credit'] = op_data['Credit'] * 2

        # 若获得积分的老爸老妈有邀请人，邀请人可以获得相同的积分
        if timestamp_CreateTime >= timestamp_1001 and "InviterID" in customer_data and op_data['Credit'] > 0 and \
                customer_data['group_id'] == 2:
            deal_inventor(customer_data, order_data, op_data['Credit'])

        if op_data['Credit'] == 0:
            op_data['Sent'] = 0
            op_data['Remark'] = msg_credit_zero
            op_data['Status'] = 6
            op_data['CustomerData'] = customer_data
        else:
            op_data['Sent'] = 1
            op_data['Remark'] = msg_eligibility
            op_data['Status'] = 1
            op_data['group_id'] = customer_data['group_id']
            op_data['CustomerData'] = customer_data
            op_data['OrderData'] = order_data
            op_data['CustomerID'] = customer_id

            if customer_type == 'other':
                tourists_issued_credit += op_data['Credit']
                if tourist_id:
                    op_data['TouristID'] = tourist_id

            do_present(order_data, customer_type, op_data)
    else:
        op_data['Sent'] = 0
        op_data['Remark'] = msg_unknown_error
        op_data['Status'] = 5
        op_data['CustomerData'] = customer_data

    if customer_data:
        if "CustomerName" in customer_data:
            op_data['CustomerName'] = customer_data['CustomerName']

    insert_auto_present_credit_log(op_data)
    print('OrderID : ', order_data['_id'], 'main_judge结果 : ', op_data['Remark'], 'customer_type:', customer_type,
          'tourist_id:', tourist_id)

    return tourists_issued_credit


def insert_auto_present_credit_log(op_data):
    unset_CustomerData_index = ['Coupons', 'CustomerP4P', 'BusinessType']
    for CustomerData_index in unset_CustomerData_index:
        try:
            op_data['CustomerData'].pop(str(CustomerData_index))
        except BaseException as err:
            pass

    unset_OrderData_index = ['Company', 'ProductCompany', 'BusinessType', 'StartAddress', 'EndAddress', 'ContractEnlarge', 'FinancesEnlarge', 'PriceEnlarge', 'PriceRecord', 'OtherEnlarge']
    for OrderData_index in unset_OrderData_index:
        try:
            op_data['OrderData'].pop(str(OrderData_index))
        except BaseException as err:
            pass

    common_data = {
        "Version": '1.6',
        "BatchTimestamp": today_time,
        "BatchDate": today,
        "AddTime": now_iso_date,
    }

    common_data.update(op_data)

    col_auto_present_credit_log.insert_one(common_data)


def remind_tag_customer(message, IM_URL):
    """
    @name       提醒私顾给客户打标签
    """
    msg = {
        'message': message
    }
    json_msg = json.dumps(msg)
    post_data = 'token=&data=' + json_msg + '&notCheckSender=true'

    url = str(IM_URL) + 'crmSendRemindBatch'

    res = requests.post(
        url=url,
        data=post_data,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )

    print('发送给客户打标签通知: ')
    print(message)
    print(res.text)


def set_remind_data(order_data):
    try:
        IntentOrderId = str(order_data['IntentOrder']['IntentOrderId'])
        if IntentOrderId == '000000000000000000000000':
            return False
    except BaseException as err:
        return False

    try:
        customer_name = str(order_data['CustomerEnlarge']['CustomerName'])
    except BaseException as err:
        customer_name = ''

    if not customer_name:
        return False

    recver = str(order_data['OwnUser']['ForeignKeyID'])
    content = '客户: ' + customer_name + ' 订单:' + str(order_data['OrderNo']) + ' 已回团, 请给客户添加标签'
    title = '客户: ' + customer_name + ' 订单:' + str(order_data['OrderNo']) + ' 产品: ' + str(order_data['ProductTitle']) + ' 已回团, 请给客户添加标签'

    return {
        'sender': '000000000000000000002251',
        'recver': recver,
        'title': title,
        'content': content,
        'targetId': IntentOrderId,
        'messageType': 2,
        'type': 32,
        'subType': 232002,
        'hasBusinessId': False,
    }


if __name__ == "__main__":
    print(today, 'auto_present_credit_in_return 脚本执行开始')

    mongodb_ip = str(ReadConfig().get_db("mongodb_ip"))
    mongodb_port = int(ReadConfig().get_db("mongodb_port"))
    mongodb_auth = str(ReadConfig().get_db("mongodb_auth"))
    mongodb_password = str(ReadConfig().get_db("mongodb_password"))
    IM_URL = str(ReadConfig().get_url("im_url"))

    # 作为CreateUserID记录
    op_user_id = '000000000000000000002251'

    # 连接MongoDB
    client = pymongo.MongoClient(mongodb_ip, mongodb_port)
    db = client.erp
    db.authenticate(mongodb_auth, mongodb_password)

    # MongoDB表
    col_orders = db.Orders
    col_customers = db.Customers
    col_auto_present_credit_log = db.AutoPresentCreditLog
    col_customer_integral_record = db.CustomerIntegralRecord
    col_customer_level_integral_record = db.CustomerLevelIntegralRecord
    col_order_tourists = db.OrderTourists

    condition = {
        'TeamEndTime': {
            '$gte': cst_tz.localize(datetime.datetime(year_yesterday, month_yesterday, day_yesterday, 0, 0, 0)),
            '$lte': cst_tz.localize(datetime.datetime(year_yesterday, month_yesterday, day_yesterday, 23, 59, 59)),
            # '$gte': datetime.datetime(year_yesterday, month_yesterday, day_yesterday, 0, 0, 0).replace(tzinfo=cst_tz),
            # '$lte': datetime.datetime(year_yesterday, month_yesterday, day_yesterday, 23, 59, 59).replace(tzinfo=cst_tz)
        },
        'DelStatus': 0,
        'OrderStatus': 3,
        'TotalNumber': {'$gt': 0},
        'TotalFinalPrice': {'$gt': 0},
        '$or': [
            {'Company.ForeignKeyID': ObjectId('000000000000000000000001')},
            {'Company.ForeignKeyID': ObjectId('000000000000000000000028')},
        ],
        '$and': [
            {'OrderSourceTypeID': {'$ne': 4}},
            {'OrderSourceTypeID': {'$ne': 26}},
        ],
    }

    order_list = col_orders.find(condition)
    message = []

    for order_data in order_list:
        try:
            if "CustomerEnlarge" in order_data and "ForeignKeyID" in order_data['CustomerEnlarge']:
                data = set_remind_data(order_data)
                if data:
                    message.append(data)

                tourists_issued_credit = 0
                main_judge(order_data['CustomerEnlarge']['ForeignKeyID'], order_data, 'main', tourists_issued_credit,
                           {})
            else:
                op_data = {
                    "CustomerType": 'none',
                    "Sent": 0,
                    "Remark": "order_data['CustomerEnlarge']['ForeignKeyID']" + msg_not_exists,
                    "Status": 8,
                    "OrderData": order_data,
                }

                insert_auto_present_credit_log(op_data)
                print('OrderID : ', order_data['_id'], '订单判断:', op_data['Remark'])
        except BaseException as err:
            op_data = {
                "CustomerType": 'none',
                "Sent": 0,
                "Remark": '异常',
                "Status": 9,
                "OrderData": order_data,
            }

            insert_auto_present_credit_log(op_data)
            print('OrderID : ', order_data['_id'], 'Exception: ', err)

    ''' 提醒私顾给客户打标签 '''
    if message:
        remind_tag_customer(message, IM_URL)

    print(today, 'auto_present_credit_in_return 脚本执行结束')
