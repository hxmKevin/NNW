# -*- coding: utf-8 -*-
"""
本接口是根据id从数据库中获取数据显示个人购物信息
接口：http://47.56.114.113:8080/nnwemall/common/personalInfoAPI
请求参数：{"userId":userId}
"""
import logging
import datetime
import json
from flask import Flask, request
from pymongo import MongoClient
from pytz import timezone
# logging.basicConfig(filename='TSTC_ExpressInfo.log',level=logging.INFO)
cst_tz = timezone('Asia/Shanghai')
year = datetime.datetime.now().year
month = datetime.datetime.now().month
day = datetime.datetime.now().day
TodayISODate = cst_tz.localize(datetime.datetime(year, month, day, 0, 0, 0))
#连接mydb数据库,账号密码认证
client = MongoClient('47.99.70.148', 27200)
db = client.erp
db.authenticate("feiyangERP", "TalkIsCheap_2015")
LogParserMONGO2 = MongoClient('mongodb://feiyangERP:TalkIsCheap_2015@47.99.70.148:27200')
app = Flask(__name__)
# loger = logging.getLogger()

# @app.route('/nnwemall/common/personalInfoAPI',methods=['POST'])
def get_data():
    unpaid = 0
    untransfered = 0
    on_the_way = 0
    uncommentted = 0
    refund = 0
    coupon = 0
    group_order = 0
    can_group = 0
    out_of_date = 0
    new_promotion = True
    new_product = True
    cart_count = 0
    year = datetime.datetime.now().year
    month = datetime.datetime.now().month
    day = datetime.datetime.now().day
    hour = datetime.datetime.now().hour
    min = datetime.datetime.now().minute
    sec = datetime.datetime.now().second
    today = datetime.date.today()
    tomorrow = today + datetime.timedelta(days=1)
    TodayISODate = cst_tz.localize(datetime.datetime(year, month, day, 0, 0, 0))
    status_code = 701
    requestID = "5e789533281426be678b4567"
    try:
        requestData = request.get_data()
        requestID = json.loads(requestData.decode())["userId"]
    except:
        status_code = 600

    try:
        #获取购物车数量
        cart_count_cursor = db["MallShoppingCart"].find({"CustomerID":requestID})
        for cart_count_item in cart_count_cursor:
            cart_count = cart_count_item["Num"]
            break
    except:
        status_code = 601
    try:
        #获取是否有上新产品
        new_product_count = db["MallProduct"].find({"UpdateTime": {"$gte": TodayISODate}}).count()
        if new_product_count ==0:
            new_product = False
        elif new_product_count >0:
            new_product = True
    except:
        status_code = 602

    try:
        #获取订单
        unpaid = db["ShoppingOrder"].find({"Customer.CustomerID":requestID,"OrderStatus":1,"PayStatus":1}).count()
        untransfered = db["ShoppingOrder"].find({"Customer.CustomerID": requestID, "OrderStatus": 1, "PayStatus": 2,"Delivery.DeliveryStatus":1}).count()
        on_the_way = db["ShoppingOrder"].find({"Customer.CustomerID": requestID, "OrderStatus": 1, "PayStatus": 2,"Delivery.DeliveryStatus":2}).count()
        uncommentted = db["ShoppingOrder"].find({"Customer.CustomerID": requestID, "OrderStatus": 1, "PayStatus": 2,"Delivery.DeliveryStatus":3}).count()
        refund = db["ShoppingOrder"].find({"Customer.CustomerID": requestID, "RefundStatus": {"$in":[1,2,3]},}).count()
    except:
        status_code = 602
    try:
        #促销活动
        NowISODate = cst_tz.localize(datetime.datetime(year, month, day, hour, min, sec))
        NowISODate_afterDay = cst_tz.localize(datetime.datetime(tomorrow.year, tomorrow.month, tomorrow.day, hour, min, sec))
        #判断可以领取的券的数量
        coupon_available_all = 0
        coupon_available_cursor = db["ShopVoucher"].find({"eTime":{"$gte": NowISODate}})
        for coupon_available_item in coupon_available_cursor:
            #1.判断是否有领取上限
            if coupon_available_item["IsTotalShipments"] == 1:
                #判断是不是还有可领取的数量
                if coupon_available_item["TotalShipments"] - coupon_available_item["TotalYLNum"] >0:
                    coupon_available = coupon_available_item["TotalShipments"] - coupon_available_item["TotalYLNum"]#当前券种的剩余券数
                    #判断是不是限制每人领取数量
                    person_coupon_available = coupon_available_item["LimitTimesType"]
                    if person_coupon_available == 1:#如果每人领取数量有上限
                        person_coupon_available_num = coupon_available_item["LimitTimes"]#每个人的领取上限
                        if person_coupon_available_num >= coupon_available:
                            coupon_available_all += coupon_available#如果上限大于等于剩余券数，则加上剩余券数
                        else:
                            coupon_available_all += person_coupon_available_num#如果上限小于剩余券数，则加上上限券数
                    else:
                        coupon_available_all += 1#加上可以领的券
                else:
                    continue
            else:
                person_coupon_available = coupon_available_item["LimitTimesType"]
                if person_coupon_available == 1:  # 如果每人领取数量有上限
                    person_coupon_available_num = coupon_available_item["LimitTimes"]  # 每个人的领取上限
                    coupon_available_all += person_coupon_available_num  # 加上上限券数
                else:
                    coupon_available_all += 1  # 加上可以领的券,bug
        coupon_get = db["ShopUseVoucherList"].find({"Useid":requestID,"eTime":{"$gte": NowISODate}}).count()#获取已经领取而且没过期的券数量
        coupon = coupon_available_all - coupon_get

        #我发起的拼团数量
        group_order = db["ShopFQAssemble"].find({"fqUserid":requestID,"eTime":{"$gte": NowISODate}}).count()#自己发起的拼团数
        can_group = db["ShopAssemble"].find({"eTime":{"$gte": NowISODate}}).count() - group_order#可以拼团的数量-自己发起的拼团
        #即将到期的券数量：已领的，一天内到期的券
        out_of_date = db["ShopUseVoucherList"].find({"Useid":requestID,"UseType":1,"uelTime":{"$gte": NowISODate,"$lt":NowISODate_afterDay}}).count()

        #判断是否有当天活动
        new_activity = db["ShopVoucher"].find({"eTime":{"$gte": NowISODate},"DeductionBase":0}).count()
        if new_activity == 0:
            new_promotion = False
        else:
            new_promotion = True
    except:
        status_code = 602

    return_data = {
        "data": {
            # 订单
            "order": {
                "unpaid": unpaid,  # 未付款数量
                "untransfered": untransfered,  # 待发货数量
                "on_the_way": on_the_way,  # 待收货数量
                "uncommentted": uncommentted,  # 待评价数量
                "refund": refund  # 售后退款数量
            },
            # 促销活动
            "promotion": {
                "my_promotion": {  # 我的活动
                    "coupon": coupon,  # 我可领的优惠券数量
                    "group_order": group_order,  # 我发起的拼团数量（有效）
                    "can_group": can_group,  # 可参与的拼团 = 总有效拼团数 - 我发起的拼团数量
                    "out_of_date": out_of_date  # 即将过期的券的数量，已领的一天内到期的（券状态1表示已领）
                },
                "new_promotion": new_promotion  # 全局是否有新的活动
            },
            # 产品
            "product": {
                "new_product": new_product  # 是否有新品上架
            },
            # 购物车
            "cart": {
                "cart_count": cart_count  # 我购物车商品数量
            }
        }
    }
    # loger.info(datetime.datetime.now(),":",{"code":status_code,"userId":requestID,"data":return_data})
    return json.dumps({"code":status_code,"userId":requestID,"data":return_data})

if __name__ == '__main__':
    # app.run(host="0.0.0.0", port=8080, debug=True)
    get_data()