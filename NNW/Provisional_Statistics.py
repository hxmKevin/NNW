#coding: utf-8
"""
临时统计用户复购率和订单复购率（基于总体）
"""
from common.handle_mongodb import HandleMongoDB
m = HandleMongoDB()
mongoConnect = m.mongodb_connect()
import datetime
from pytz import timezone
cst_tz = timezone('Asia/Shanghai')
OrderTimeISODate_begin_total = cst_tz.localize(datetime.datetime(2020, 2, 17, 17, 0, 0))
OrderTimeISODate_end_total = cst_tz.localize(datetime.datetime(2020, 3, 21, 17, 0, 0))
def phone():
    cursor = mongoConnect["NNWOrder"].aggregate(
                [
                    {
                        "$match": {
                            "DelStatus": 0,
                            "PayStatus": 2,
                            "chan3_pin3_ID": {"$ne": "4056068"},
                            "SalesVolume": {"$gt": 0},
                            'OrderTimeISODate': {
                                '$gte': OrderTimeISODate_begin_total,
                                '$lt': OrderTimeISODate_end_total,
                            },
                        }
                    },
                    {
                        "$group": {
                            "_id": "$you2_ke4_shou3_ji1_hao4",
                            "phone_num_total": {"$sum": 1},
                            "name": {"$first": "$you2_ke4_xing4_ming2"}

                        }
                    }
                ]
            )
    passengerList = []
    for item in cursor:
        mongoConnect["NNWphoneN"].insert(item)
    #     phone = item["lian2_xi4_ren2_shou3_ji1_hao4"]
    #     passengerList.append(phone)
    # print(len(passengerList))
#订单复购率统计
#用户复购率统计
def order():
    cursor = mongoConnect["NNWphoneN"].find()
    order_total = 0
    for item in cursor:
        order_total+=item["phone_num_total"]

    print(order_total)

    cursor = mongoConnect["NNWphoneN"].find({"phone_num_total":{"$gt":1}})
    order_total_2 = 0
    x = 0
    for item in cursor:
        order_total_2 += item["phone_num_total"]
        x+=1
    print(order_total_2-x)


if __name__ == '__main__':

    phone()
    order()