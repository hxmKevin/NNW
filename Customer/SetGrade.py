#!/usr/bin/env python
# encoding:UTF-8
import pytz
from bson import ObjectId
import datetime
import sys
import os
import requests
sys.path.append(os.path.abspath(os.path.dirname(__file__) + '/' + '../'))
from Config.DB import MilestoneMONGO
from Config.CONST import *
import json
import time
import pymysql

pacific = pytz.timezone('PRC')

def setExpCustomer():
    Customers = MilestoneMONGO['db'].Customers.find({
        'group_id':2,
        'DelStatus':0,
        'ExpTime':{'$lte':pacific.localize(datetime.datetime.now())},
        "group_lv":{"$exists":False}
        })
    for customer in Customers:
        setCustomerGrade(customer,1,0)

def getUpgradeCustomer():
    # 升级为老班长
    inviterCounts = MilestoneMONGO['db'].Customers.aggregate([
        {'$match':{'group_id':2,'DelStatus':0,'InviterTime':{'$gte':pacific.localize((datetime.datetime.now() + datetime.timedelta(days=-365)))}}},
        {'$group': {'_id': '$InviterID', 'counts': {'$sum': 1}}},
        {'$match': {'counts': {'$gte': 10}}},
    ])
    for inviter in inviterCounts:
        customer = getCustomerInfo(inviter['_id'])
        if customer != None and 'group_id' in customer.keys() and customer['group_id'] == 2 and ('group_lv' not in customer.keys() or customer['group_lv'] == 0):
            setCustomerGrade(customer, 0, 1)
            print(customer['_id'])
    # 升级为理事长
    orders = MilestoneMONGO['db'].Orders.aggregate([
        {'$match':{'OrderStatus':3,'TeamEndTime':{'$gt':pacific.localize(datetime.datetime.now()+datetime.timedelta(days=-3)),'$lt':pacific.localize(datetime.datetime.now())}}},
        {'$group': {'_id': '$CustomerEnlarge.ForeignKeyID', 'Number': {'$sum': '$TotalNumber'}, 'Price': {'$sum': '$TotalFinalPrice'}}},
        {'$match': {'$or':[{'Number': {'$gte': 200},'Price': {'$gte': 150000}},{'Price': {'$gte': 200000}}]}},
    ])
    for o in orders:
        customer = getCustomerInfo(o['_id'])
        if customer!= None and 'group_id' in customer.keys() and customer['group_id']==2 :
            jointime = getCustomerJoinTime(customer['_id'])
            if jointime > (datetime.datetime.now()+datetime.timedelta(days=-365)):
                selecttime = jointime
            else:
                selecttime = datetime.datetime.now()+datetime.timedelta(days=-365)
            neworders = MilestoneMONGO['db'].Orders.aggregate([
                {'$match': {'OrderStatus': 3, 'TeamEndTime': {
                    '$gt': pacific.localize(selecttime),
                    '$lt': pacific.localize(datetime.datetime.now())},'CustomerEnlarge.ForeignKeyID':customer['_id']}},
                {'$group': {'_id': '$CustomerEnlarge.ForeignKeyID', 'Number': {'$sum': '$TotalNumber'},
                            'Price': {'$sum': '$TotalFinalPrice'}}},
                {'$match': {
                    '$or': [{'Number': {'$gte': 200}, 'Price': {'$gte': 150000}}, {'Price': {'$gte': 200000}}]}},
            ])
            if 'group_lv' not in customer.keys():
                fromData = 0
            else:
                fromData = customer['group_lv']
            if neworders[0]!=None:
                setCustomerGrade(customer, fromData, 2)
                print(customer['_id'])

def getCustomerInfo(id):
    customer = MilestoneMONGO['db'].Customers.find_one({'_id':id})
    return customer

def setCustomerGrade(customer,fromData,toData):
    # 老班长
    if customer['group_id']==2:
        update = {'group_lv':toData}
        if toData==0:
            checkStatus=0
        elif toData==1:
            checkStatus=1
        elif toData==2:
            checkStatus=2
        # checkStatus 表示等级变更状态
        if toData != fromData:
            update['ExpTime'] = pacific.localize(datetime.datetime.now() + datetime.timedelta(days=365))
            log(customer['_id'], fromData, toData)
        MilestoneMONGO['db'].Customers.update_one({'_id':customer['_id']},{'$set':update})
        sendMessage(checkStatus,customer)

def sendMessage(checkStatus,customer):
    print(checkStatus)
    url = url=MessageUrl+'/crmSendRemindBatch'
    messageData = {
            "customerName":customer['CustomerName'],
            "CustomerMobile":customer['CustomerMobile'],
            "CustomerID":str(customer['_id']),
            "checkStatus":checkStatus
        }
    data = {
        "message":[{
        'applyUserID':"000000000000000000002251",
        "applyUserName":"系统管理员",
        "applyUserDeptID":"000000000000000000000781",
        "recver":"000000000000000000001671",
        "data":json.dumps(messageData),
        "title":"",
        "content":"test",
        "targetId":"000000000000000000000000",
        "type":32,
        "subType":232001,
        "messageType":2
    }]
    }
    title = ""
    if checkStatus==0:
        title = customer['CustomerName']+"["+customer['CustomerMobile']+"]"+"等级变更为老爸老妈"
    elif checkStatus == 1:
        title = customer['CustomerName']+"["+customer['CustomerMobile']+"]"+"等级变更为老班长"
    elif checkStatus == 1:
        title = customer['CustomerName'] + "[" + customer['CustomerMobile'] + "]" + "等级变更为理事长"
    data['message'][0]['title'] = title
    data['message'][0]['content'] = title
    result = requests.post(url=url,data={'notCheckSender':True,'data':json.dumps(data)},headers={'Content-Type':'application/x-www-form-urlencoded'})
    print(result.text)

def log(id,fromData,toData):
    data = {
        'CustomerID':id,
        'FromGrade':fromData,
        'ToGrade':toData,
        'note':'会员时间到期',
        'time':pacific.localize(datetime.datetime.now()),
        'source':2
    }
    MilestoneMONGO['db'].CustonmerGradeChangeLog.insert_one(data)

def findSendCouponCoustomer():
    customers = MilestoneMONGO['db'].CustomerCouponLog.find({'status':0,'nexttime':{'$lt':pacific.localize(datetime.datetime.now())}})
    for customer in customers:
        sendCoupon(customer.CustomerID)

def sendCoupon(customerID):
    url = url=IFLYING_API_URL+'/user/grantGroupCoupon'
    result = requests.post(url=url,data={'UID':customerID},headers={'Content-Type':'application/x-www-form-urlencoded'})
    return result.text

def initExpCustomer():
    Customers = MilestoneMONGO['db'].Customers.find({'group_id':{'$in':[2,3]},'DelStatus':0,'group_lv':{'$ne':1}})
    for customer in Customers:
        setCustomerGrade(customer,0,0)

def initfindSendCouponCoustomer(start):
    length = 200
    customers = MilestoneMONGO['db'].Customers.find({'group_id':{'$in':[2,3]},'DelStatus':0}).skip(start).limit(length).sort("_id")
    try:
        hasData = False
        for customer in customers:
            hasData = True
            rjson = sendCoupon(customer['_id'])
            rdata = json.loads(rjson)
            if rdata['result'] != 1:
                print(customer['_id'], " failed!")
                raise Exception("error!")
    except:
        print(start,"-",(start+length)," Execution error! wait for 15 seconds!")
        time.sleep(15)
        initfindSendCouponCoustomer(start)
    else:
        if hasData == True:
            print(start, "-", (start + length), ' mission complete')
            time.sleep(5)
            start = start + length
            initfindSendCouponCoustomer(start)
        else:
            print('init mission complete!!!!')
            quit()

def getCustomerJoinTime(id):
    # 打开数据库连接
    db = pymysql.connect("121.40.53.105", "hengyue", "mysql-hengyue", "Customer2018")

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 查询语句
    sql = "SELECT * FROM member_group_lbz WHERE member_object_id = \'%s\'" % (id)
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取记录
        results = cursor.fetchone()
        return results[4]
    except:
        return 0

    # 关闭数据库连接
    db.close()

def getDowngradeCustomer():
    # 降级为老爸老妈
    CustomerList_one = MilestoneMONGO['db'].Customers.find({
        'group_id':2,
        'DelStatus':0,
        'group_lv':1,
        'ExpTime':{'$gt':pacific.localize(datetime.datetime.now()+datetime.timedelta(days=-3)),'$lt':pacific.localize(datetime.datetime.now())}
    })
    for downCus_one in CustomerList_one:
        inviterCounts = MilestoneMONGO['db'].Customers.count({
            'group_id': 2, 'DelStatus': 0,
            'InviterTime': {
                '$gte': pacific.localize((datetime.datetime.now() + datetime.timedelta(days=-365)))},
            'InviterID':downCus_one['_id']
        })
        if inviterCounts < 10:
            setCustomerGrade(downCus_one, 1, 0)
            print(downCus_one['_id'])
    # 降级为老班长
    CustomerList_two = MilestoneMONGO['db'].Customers.find({
        'group_id': 2,
        'DelStatus': 0,
        'group_lv': 2,
        'ExpTime': {'$gt': pacific.localize(datetime.datetime.now() + datetime.timedelta(days=-3)),
                    '$lt': pacific.localize(datetime.datetime.now())}
    })
    for downCus_two in CustomerList_two:
        neworders = MilestoneMONGO['db'].Orders.aggregate([
            {'$match': {'OrderStatus': 3, 'TeamEndTime': {
                '$gt': pacific.localize(datetime.datetime.now() + datetime.timedelta(days=-365)),
                '$lt': pacific.localize(datetime.datetime.now())},'CustomerEnlarge.ForeignKeyID':downCus_two['_id']}},
            {'$group': {'_id': '$CustomerEnlarge.ForeignKeyID', 'Number': {'$sum': '$TotalNumber'},
                        'Price': {'$sum': '$TotalFinalPrice'}}},
            {'$match': {
                '$or': [{'Number': {'$gte': 200}, 'Price': {'$gte': 150000}}, {'Price': {'$gte': 200000}}]}},
        ])
        if neworders[0]!=None:
            setCustomerGrade(downCus_two, 2, 1)
            print(downCus_two['_id'])

# initExpCustomer()
# initfindSendCouponCoustomer(0)
setExpCustomer()
# setCustomerGrade(ObjectId('000000000000000000208502'),0,1)
getUpgradeCustomer()
getDowngradeCustomer()
#findSendCouponCoustomer()
# sendCoupon('000000000000000000208502')
# sendMessage(0,0)