# coding: utf-8
import os
import sys
from pymongo import UpdateOne

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from common import handle_mongodb
from common.readconfig import ReadConfig
import traceback
import requests
import random
from bson import json_util
from bson.objectid import ObjectId
from datetime import date,timedelta
import time
import datetime
import json
import xlrd
from xlrd import xldate_as_tuple, xldate_as_datetime
from pytz import timezone
from xpinyin import Pinyin
import xlwt
import operator
#快递导出的时间段
hourEveryDay = 12
cst_tz = timezone('Asia/Shanghai')


class OrderStat:
    def __init__(self):
        self.mongodb = handle_mongodb.HandleMongoDB()
        self.mongodb.mongodb_connect()
        self.file_path = str(ReadConfig().get_nnw_param("file_path"))

        cst_tz = timezone('Asia/Shanghai')
        self.now_iso_date = cst_tz.localize(datetime.datetime.now())
        self.year = datetime.datetime.now().year
        self.month = datetime.datetime.now().month
        self.day = datetime.datetime.now().day
        self.pre_year = (date.today() + timedelta(days=-1)).year
        self.pre_month = (date.today() + timedelta(days=-1)).month
        self.pre_day = (date.today() + timedelta(days=-1)).day
        self.pre_7_year = (date.today() + timedelta(days=-7)).year
        self.pre_7_month = (date.today() + timedelta(days=-7)).month
        self.pre_7_day = (date.today() + timedelta(days=-7)).day

        self.col_NNWOrder = self.mongodb.select_col('NNWOrder')
        self.col_Personnel = self.mongodb.select_col('Personnel')
        self.col_NNWIdName = self.mongodb.select_col('NNWIdName')
        self.col_NNWCustomer = self.mongodb.select_col('NNWCustomer')
        self.col_NNWGroup = self.mongodb.select_col('NNWGroup')

        self.id_name = {}
        # self.OrderTimeISODate_begin = cst_tz.localize(datetime.datetime(2020, 3, 11, 17, 0, 0))
        if datetime.datetime.now().hour == hourEveryDay:
            hour = 12
        else:
            hour = 17
        self.OrderTimeISODate_begin = cst_tz.localize(datetime.datetime(self.pre_year, self.pre_month, self.pre_day, hour, 0, 0))
        # self.OrderTimeISODate_begin = cst_tz.localize(datetime.datetime(2020, 3, 1, 14, 0, 0))

        self.OrderTimeISODate_end = cst_tz.localize(datetime.datetime(self.year, self.month, self.day, hour, 0, 0))
        # self.OrderTimeISODate_end = cst_tz.localize(datetime.datetime(2020, 3, 12, 17, 0, 0))
        self.OrderTimeISODate_begin_7 = cst_tz.localize(datetime.datetime(self.pre_7_year, self.pre_7_month, self.pre_7_day, 17, 0, 0))
        # self.OrderTimeISODate_begin = cst_tz.localize(datetime.datetime(2020, 3, 1, 14, 0, 0))

        self.OrderTimeISODate_begin_total_newGroup = cst_tz.localize(datetime.datetime(2020, 3, 15, 17, 0, 0))
        self.OrderTimeISODate_begin_total = cst_tz.localize(datetime.datetime(2020, 2, 17, 17, 0, 0))
        self.OrderTimeISODate_end_total = cst_tz.localize(datetime.datetime(self.year, self.month, self.day, 17, 0, 0))
        # self.OrderTimeISODate_end_total = cst_tz.localize(datetime.datetime(2020, 3, 12, 17, 0, 0))
        self.style_column_bkg = ''
        self.supplier = []
        self.total_account_object_before_217 = {}
        self.total_account_object = {}
        self.total_account_object_i_r = {}
        self.total_account_object_newGroup = {}

    def main(self):
        self.check_import()

        # self.export_item_stat()

        # self.import_excel_two()
        self.export_excel_two()

        # self.export_item_stat()

        # self.login()

        # self.import_personnel_two()

        # self.transfer_belong()
        # self.stat_personnel_cumulative_turnover()

        # 将下单人id更新到订单中
        # self.import_order_customer_id()

        # 导入上级id
        # self.import_superior_id()

        # 更新NNWOrder 表 销量
        # self.update_SalesVolume()
        # self.update_BusinessTurnover()
        # self.update_belong_data()
        # self.update_BusinessTurnoverCost()
        # self.update_GrossProfit()

        # self.update_group_id()

    def check_import(self):
        res = self.col_NNWOrder.aggregate(
            [
                {
                    '$match': {
                        'DelStatus': 0,
                    },
                },
                {
                    '$project':
                        {
                            'xia4_dan1_shi2_jian1': 1,
                            'OrderTimeISODate': 1,
                        }
                },
                {
                    '$sort': {'OrderTimeISODate': -1}
                },
                {
                    '$limit': 1
                }
            ]
        )

        for one in res:
            print('最新下单时间', one['xia4_dan1_shi2_jian1'])
            break

        temp_condition = {
            'DelStatus': 0,
            "PayStatus": 2,
            "chan3_pin3_ID": {"$ne": "4056068"},
            "SalesVolume": {"$gt": 0},
            'OrderTimeISODate': {
                '$gte': self.OrderTimeISODate_begin,
                '$lt': self.OrderTimeISODate_end,
            },
            'BelongName': '',
        }

        temp_project = {
            'ID': 1,
            'OrderCustomerId': 1,
            'OrderCustomerName': 1,
            'fen1_xiang3_yuan2_zhang4_hao4': 1,
            'fen1_xiang3_yuan2_ID': 1,
            'xia4_dan1_shi2_jian1': 1,
        }

        res = self.col_NNWOrder.find(temp_condition, temp_project)

        print('')
        print('无归属订单')
        for one in res:
            try:
                print('ID:'+one['ID']+' '+'下单人ID:'+one['OrderCustomerId']+' '
                      +' '+'分享员ID:'+one['fen1_xiang3_yuan2_ID']+' '+'分享员账号:'+one['fen1_xiang3_yuan2_zhang4_hao4']+' '
                      +'下单时间:'+one['xia4_dan1_shi2_jian1'])
            except:
                print(json_util.dumps(one, ensure_ascii=False))
                exit()

    def export_item_stat(self):
        res = self.col_NNWOrder.aggregate(
            [
                {
                    "$match": {
                        "DelStatus": 0,
                        "PayStatus": 2,
                        "chan3_pin3_ID": {"$ne": "4056068"},
                        "SalesVolume": {"$gt": 0},
                        'OrderTimeISODate': {
                            '$gte': self.OrderTimeISODate_begin_total,
                            '$lt': self.OrderTimeISODate_end_total,
                        },
                    }
                },
                {
                    "$group": {
                        "_id": "$chan3_pin3_ID",
                        "order_num_total": {"$sum": 1},
                        "sell_num_total": {"$sum": "$SalesVolume"},
                        "BusinessTurnover_total": {"$sum": "$BusinessTurnover"},
                        "BusinessTurnoverCost_total": {"$sum": "$BusinessTurnoverCost"},
                        "GrossProfit_total": {"$sum": "$GrossProfit"},
                        "name": {"$first": "$ming2_cheng1"},
                        "chan3_pin3_ID": {"$first": "$chan3_pin3_ID"},
                        "skuname": {"$first": "$SKU_ming2_cheng1"}
                    }
                }
            ]
        )

        goods_stat_total_dict = {}
        for one in res:
            goods_id = goods_id
            goods_stat_total_dict[goods_id] = one

            temp_res = self.col_NNWOrder.aggregate(
                [
                    {
                        "$match": {
                            "chan3_pin3_ID": goods_id,
                            "DelStatus": 0,
                            "PayStatus": 2,
                            "SalesVolume": {"$gt": 0}
                        }
                    },
                    {
                        "$group": {
                            "_id": "$you2_ke4_shou3_ji1_hao4",
                            "repeat_order_num": {"$sum": 1},
                            "chan3_pin3_ID": {"$first": "$chan3_pin3_ID"},
                        }
                    },
                    {
                        "$match": {
                            "repeat_order_num": {"$gt": 1}
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "repeat_order_num": {"$sum": 1},
                        }
                    },
                ]
            )
            # print(json_util.dumps(one, ensure_ascii=False))
            repeat_order_num = 0
            for temp_one in temp_res:
                repeat_order_num = temp_one['repeat_order_num']
                break

            goods_stat_total_dict[goods_id]['repeat_num'] = repeat_order_num

        res = self.col_NNWOrder.aggregate(
            [
                {
                    "$match": {
                        "DelStatus": 0,
                        "PayStatus": 2,
                        "chan3_pin3_ID": {"$ne": "4056068"},
                        "SalesVolume": {"$gt": 0},
                        'OrderTimeISODate': {
                            '$gte': self.OrderTimeISODate_begin,
                            '$lt': self.OrderTimeISODate_end,
                        },
                    }
                },
                {
                    "$group": {
                        "_id": "$chan3_pin3_ID",
                        "order_num": {"$sum": 1},
                        "sell_num": {"$sum": "$SalesVolume"},
                        "BusinessTurnover": {"$sum": "$BusinessTurnover"},
                        "BusinessTurnoverCost": {"$sum": "$BusinessTurnoverCost"},
                        "GrossProfit": {"$sum": "$GrossProfit"},
                        "name": {"$first": "$ming2_cheng1"},
                        "chan3_pin3_ID": {"$first": "$chan3_pin3_ID"},
                        "skuname": {"$first": "$SKU_ming2_cheng1"}
                    }
                }
            ]
        )

        goods_dict = {}
        for one in res:
            goods_id = str(one['chan3_pin3_ID'])
            goods_dict[goods_id] = one

            # goods_dict[goods_id]['order_num_total'] = goods_stat_total_dict[goods_id]['order_num_total']
            # goods_dict[goods_id]['sell_num_total'] = goods_stat_total_dict[goods_id]['sell_num_total']
            # goods_dict[goods_id]['BusinessTurnover_total'] = goods_stat_total_dict[goods_id]['BusinessTurnover_total']
            # goods_dict[goods_id]['BusinessTurnoverCost_total'] = goods_stat_total_dict[goods_id]['BusinessTurnoverCost_total']
            # goods_dict[goods_id]['GrossProfit_total'] = goods_stat_total_dict[goods_id]['GrossProfit_total']

        for d in goods_stat_total_dict:
            if d in goods_dict:
                goods_stat_total_dict[d]['repeat_num'] = goods_dict[d]['repeat_num']
                goods_stat_total_dict[d]['order_num'] = goods_dict[d]['order_num']
                goods_stat_total_dict[d]['sell_num'] = goods_dict[d]['sell_num']
                goods_stat_total_dict[d]['BusinessTurnover'] = goods_dict[d]['BusinessTurnover']
                goods_stat_total_dict[d]['BusinessTurnoverCost'] = goods_dict[d]['BusinessTurnoverCost']
                goods_stat_total_dict[d]['GrossProfit'] = goods_dict[d]['GrossProfit']
            else:
                goods_stat_total_dict[d]['order_num'] =0
                goods_stat_total_dict[d]['sell_num'] = 0
                goods_stat_total_dict[d]['BusinessTurnover'] = 0
                goods_stat_total_dict[d]['BusinessTurnoverCost'] = 0
                goods_stat_total_dict[d]['GrossProfit'] = 0
        # day = 17
        # while True:
        #     day += 1
        #     res = self.col_NNWOrder.aggregate(
        #         [
        #             {
        #                 "$match": {
        #                     "chan3_pin3_ID": "4071020",
        #                     "DelStatus": 0,
        #                     "PayStatus": 2,
        #                     'OrderTimeISODate': {
        #                         '$gte': cst_tz.localize(datetime.datetime(2020, 2, day-1, 17, 0, 0)),
        #                         '$lt': cst_tz.localize(datetime.datetime(2020, 2, day, 17, 0, 0)),
        #                     },
        #                 }
        #             },
        #             {
        #                 "$group": {
        #                     "_id": "$chan3_pin3_ID",
        #                     "ordernum": {"$sum": 1},
        #                     "sellnum": {"$sum": "$SalesVolume"},
        #                     "name": {"$first": "$ming2_cheng1"},
        #                 }
        #             }
        #         ]
        #     )
        #
        #     print(json_util.dumps(res, ensure_ascii=False))
        #
        #     if day == 29:
        #         break

    def update_belong_data(self):
        temp_condition = {
            'ID': {'$in': ['232908449','232908989','232909841','232910012']},
        }

        self.col_NNWOrder.update_many(
            temp_condition,
            {
                "$set": {
                    'BelongName': '蒋林军',
                    "BelongId": "29214725",
                    'BelongGroup': '其他',
                    'BelongGroupId': 22,
                    'BelongGroupClass': 3,
                    "JudgeBelongType": {
                        "TypeId": 5,
                        "Type": "手动调整",
                        "Remark": "张栋说是蒋林军亲友"
                    },
                }
            },
        )

    def login(self):
        url = 'http://fenxiao.feiyang.cn/Admin/Index/login.json'
        data = {'account': 'zijia001', 'password': '123456'}


        session = requests.session()

        req_header = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
        }

        # 使用session发起请求
        response = session.post(url, headers=req_header, data=data)

        if response.status_code == 200:

            url = 'http://fenxiao.feiyang.cn/Admin/Index/?login=success'

            response = session.get(url, headers=req_header)

            if response.status_code == 200:
                d = session.cookies.get_dict()
                php_session_id = d['PHPSESSID']
                print(php_session_id)
                exit()

    def stat_personnel_cumulative_turnover(self):
        """
        计算 累积营业额
        :return:
        """
        self.cal_ts_cumulative_newGroup()
        self.cal_ts_cumulative()
        self.cal_ts_cumulative_refunded()
        self.cal_cumulative_before_217()

    def cal_cumulative_before_217(self):
        json_before_217 = [{"name": "吴滨", "num": "171", "account": "13385.6"},
                           {"name": "章丹丹", "num": "87", "account": "10777"},
                           {"name": "何斌锋", "num": "101", "account": "9192"},
                           {"name": "周璐露", "num": "78", "account": "9591"},
                           {"name": "裘郑", "num": "57", "account": "6873"},
                           {"name": "林莉", "num": "59", "account": "6662"},
                           {"name": "陈晓冬", "num": "69", "account": "6455"},
                           {"name": "张金君", "num": "62", "account": "7356"},
                           {"name": "罗曼青", "num": "44", "account": "5518"},
                           {"name": "汪叶群", "num": "48", "account": "6095"},
                           {"name": "徐松松", "num": "47", "account": "5246"},
                           {"name": "林杰", "num": "54", "account": "5076"},
                           {"name": "李达", "num": "38", "account": "4916"},
                           {"name": "周佳贤", "num": "52", "account": "4810"},
                           {"name": "鲍玖玲", "num": "30", "account": "4731"},
                           {"name": "吴鑫", "num": "43", "account": "4409"},
                           {"name": "张潇健", "num": "60", "account": "4397"},
                           {"name": "钱咪咪", "num": "41", "account": "4139"},
                           {"name": "顾立波", "num": "34", "account": "4062"},
                           {"name": "朱晓丽", "num": "43", "account": "4240"},
                           {"name": "张栋", "num": "33", "account": "3856"},
                           {"name": "陈美娣", "num": "45", "account": "3637"},
                           {"name": "朱金晶", "num": "25", "account": "3568"},
                           {"name": "袁乐", "num": "33", "account": "3549"},
                           {"name": "徐升", "num": "37", "account": "4749"},
                           {"name": "陈胡叶", "num": "37", "account": "3430"},
                           {"name": "钟晓蓉", "num": "29", "account": "3424"},
                           {"name": "周黎民", "num": "37", "account": "3134"},
                           {"name": "陈诗瑜", "num": "35", "account": "2918"},
                           {"name": "石茗菲", "num": "27", "account": "3832"},
                           {"name": "金云", "num": "35", "account": "2824"},
                           {"name": "俞文文", "num": "34", "account": "2784"},
                           {"name": "胡丹珠", "num": "27", "account": "2708"},
                           {"name": "张立峰", "num": "28", "account": "2863"},
                           {"name": "俞珊珊", "num": "35", "account": "2647"},
                           {"name": "沈洁", "num": "26", "account": "2631"},
                           {"name": "徐涯丹", "num": "27", "account": "2837"},
                           {"name": "戚天恩", "num": "34", "account": "2391"},
                           {"name": "姚婷婷", "num": "25", "account": "2391"},
                           {"name": "陈健", "num": "22", "account": "2367"},
                           {"name": "吕燕华", "num": "21", "account": "2340"},
                           {"name": "竺洁莹", "num": "24", "account": "2262"},
                           {"name": "麻胜巍", "num": "20", "account": "2234"},
                           {"name": "周惠叶", "num": "25", "account": "2205"},
                           {"name": "王展轩", "num": "20", "account": "2514"},
                           {"name": "方烨", "num": "8", "account": "2152"},
                           {"name": "谢爱娜", "num": "24", "account": "2137"},
                           {"name": "郑笛", "num": "22", "account": "2027"},
                           {"name": "李舟瑶", "num": "22", "account": "2023"},
                           {"name": "金玲玲", "num": "12", "account": "2001"},
                           {"name": "陈园园", "num": "22", "account": "1865"},
                           {"name": "陈瑶", "num": "23", "account": "1966"},
                           {"name": "蔡佳丽", "num": "14", "account": "1710"},
                           {"name": "奚挺", "num": "22", "account": "1700"},
                           {"name": "高红菲", "num": "21", "account": "1890"},
                           {"name": "张盛", "num": "18", "account": "1685"},
                           {"name": "许一金", "num": "21", "account": "1668"},
                           {"name": "邓祥鹏", "num": "18", "account": "1646"},
                           {"name": "章亦莉", "num": "14", "account": "1642"},
                           {"name": "任珮源", "num": "20", "account": "1612"},
                           {"name": "庄艳", "num": "20", "account": "1595"},
                           {"name": "吴丰", "num": "9", "account": "1569"},
                           {"name": "苏婵", "num": "16", "account": "1566"},
                           {"name": "赵辉", "num": "12", "account": "1431"},
                           {"name": "乐林乐", "num": "16", "account": "1411"},
                           {"name": "陈英", "num": "8", "account": "1406"},
                           {"name": "吴建芸", "num": "12", "account": "1404"},
                           {"name": "干慎跃", "num": "14", "account": "1394"},
                           {"name": "杨益娜", "num": "19", "account": "1360"},
                           {"name": "徐旭", "num": "16", "account": "1338"},
                           {"name": "顾佳玲", "num": "14", "account": "1597"},
                           {"name": "陈云云", "num": "11", "account": "1299"},
                           {"name": "徐家平", "num": "17", "account": "1236"},
                           {"name": "沈婷婷", "num": "13", "account": "1212"},
                           {"name": "赵静静", "num": "10", "account": "1203"},
                           {"name": "张越", "num": "17", "account": "1201"},
                           {"name": "孙一帅", "num": "15", "account": "1198"},
                           {"name": "刘丽洁", "num": "8", "account": "1197"},
                           {"name": "王利", "num": "17", "account": "1194"},
                           {"name": "朱长江", "num": "13", "account": "1184"},
                           {"name": "陈晓春", "num": "14", "account": "1164"},
                           {"name": "周挺", "num": "17", "account": "1156"},
                           {"name": "李丽梅", "num": "17", "account": "1542"},
                           {"name": "董沁沁", "num": "6", "account": "1140"},
                           {"name": "卢丽娜", "num": "13", "account": "1122"},
                           {"name": "罗方舟", "num": "11", "account": "1116"},
                           {"name": "谢晓丹", "num": "13", "account": "1107"},
                           {"name": "徐丽娜", "num": "8", "account": "1078"},
                           {"name": "曾佳雯", "num": "9", "account": "1070"},
                           {"name": "虞勤玲", "num": "13", "account": "1065"},
                           {"name": "严波梅", "num": "10", "account": "1026"},
                           {"name": "陈丹", "num": "12", "account": "1026"},
                           {"name": "刘思彤", "num": "7", "account": "1020"},
                           {"name": "毛丽波", "num": "12", "account": "1020"},
                           {"name": "周丽萍", "num": "13", "account": "1014"},
                           {"name": "陈思瑾", "num": "11", "account": "1038"},
                           {"name": "毛余雅", "num": "10", "account": "1005"},
                           {"name": "陈文威", "num": "9", "account": "999"}, {"name": "任墺娜", "num": "3", "account": "985"},
                           {"name": "符玲霞", "num": "12", "account": "972"},
                           {"name": "叶巧丹", "num": "12", "account": "965"},
                           {"name": "周佳艳", "num": "9", "account": "932"}, {"name": "邵飞国", "num": "8", "account": "931"},
                           {"name": "余红", "num": "13", "account": "918"}, {"name": "沈颖", "num": "10", "account": "874"},
                           {"name": "宓方明", "num": "10", "account": "839"}, {"name": "徐蕾", "num": "8", "account": "819"},
                           {"name": "吴慧雯", "num": "7", "account": "804"},
                           {"name": "张静娜", "num": "11", "account": "795"},
                           {"name": "吴烨运", "num": "9", "account": "774"}, {"name": "柯园园", "num": "8", "account": "765"},
                           {"name": "杨嘉帅", "num": "10", "account": "758"},
                           {"name": "王亚琴", "num": "7", "account": "756"}, {"name": "陈立", "num": "7", "account": "746"},
                           {"name": "徐银珠", "num": "9", "account": "729"}, {"name": "梅晶晶", "num": "7", "account": "724"},
                           {"name": "卓碧雅", "num": "9", "account": "714"},
                           {"name": "胡丽清", "num": "10", "account": "906"},
                           {"name": "胡伟雄", "num": "7", "account": "705"}, {"name": "柴祥华", "num": "8", "account": "699"},
                           {"name": "周维", "num": "9", "account": "694"}, {"name": "曹晴晴", "num": "10", "account": "694"},
                           {"name": "王飘", "num": "5", "account": "682"}, {"name": "陈锡锋", "num": "5", "account": "642"},
                           {"name": "吴毓翔", "num": "6", "account": "825"}, {"name": "柳杰", "num": "7", "account": "625"},
                           {"name": "李敏", "num": "9", "account": "609"}, {"name": "苗婧婧", "num": "4", "account": "596"},
                           {"name": "陈周舟", "num": "8", "account": "564"}, {"name": "李晓俊", "num": "5", "account": "558"},
                           {"name": "吴敏", "num": "6", "account": "553"}, {"name": "陆妃娅", "num": "5", "account": "537"},
                           {"name": "张灵佳", "num": "6", "account": "518"}, {"name": "许佳玲", "num": "4", "account": "505"},
                           {"name": "张超", "num": "4", "account": "504"}, {"name": "石维", "num": "7", "account": "501"},
                           {"name": "叶佳丽", "num": "5", "account": "490"}, {"name": "金晓丽", "num": "4", "account": "486"},
                           {"name": "尤美丽", "num": "3", "account": "476"}, {"name": "李宁", "num": "6", "account": "457"},
                           {"name": "朱希", "num": "6", "account": "451"}, {"name": "宋仙红", "num": "6", "account": "451"},
                           {"name": "余兴凯", "num": "7", "account": "450"}, {"name": "高文斌", "num": "5", "account": "450"},
                           {"name": "王健", "num": "4", "account": "447"}, {"name": "陈丽娜", "num": "6", "account": "714"},
                           {"name": "林靖", "num": "5", "account": "436"}, {"name": "吴梦梦", "num": "4", "account": "434"},
                           {"name": "王亮飞", "num": "4", "account": "360"}, {"name": "张蓓蓓", "num": "5", "account": "353"},
                           {"name": "陈亚巧", "num": "4", "account": "349"}, {"name": "冯杨", "num": "4", "account": "344"},
                           {"name": "谢晴雯", "num": "5", "account": "339"}, {"name": "朱冰蕊", "num": "4", "account": "338"},
                           {"name": "陈莉雯", "num": "5", "account": "333"}, {"name": "何赛月", "num": "5", "account": "330"},
                           {"name": "严梦莹", "num": "3", "account": "327"}, {"name": "夏至诚", "num": "5", "account": "327"},
                           {"name": "洪莹", "num": "4", "account": "312"}, {"name": "程琳娜", "num": "3", "account": "307"},
                           {"name": "肖艺", "num": "4", "account": "297"}, {"name": "周炜", "num": "4", "account": "294"},
                           {"name": "潘施克", "num": "4", "account": "282"}, {"name": "吕浩峰", "num": "3", "account": "278"},
                           {"name": "叶建峰", "num": "4", "account": "276"}, {"name": "虞云祥", "num": "3", "account": "270"},
                           {"name": "华英达", "num": "3", "account": "270"}, {"name": "金建芳", "num": "4", "account": "270"},
                           {"name": "毛淑英", "num": "4", "account": "267"}, {"name": "薛娇", "num": "2", "account": "264"},
                           {"name": "毛慧娜", "num": "4", "account": "250"}, {"name": "伍宣宇", "num": "3", "account": "249"},
                           {"name": "杜欣毅", "num": "3", "account": "249"}, {"name": "胡春燕", "num": "3", "account": "228"},
                           {"name": "傅栋静", "num": "3", "account": "228"}, {"name": "张丽", "num": "3", "account": "219"},
                           {"name": "金伟健", "num": "3", "account": "216"}, {"name": "孙益", "num": "3", "account": "204"},
                           {"name": "华梦阳", "num": "3", "account": "204"}, {"name": "陈建栋", "num": "3", "account": "198"},
                           {"name": "盛小楠", "num": "3", "account": "198"}, {"name": "何莲", "num": "3", "account": "198"},
                           {"name": "徐梦威", "num": "1", "account": "198"}, {"name": "高莎莎", "num": "3", "account": "198"},
                           {"name": "吴灵紫", "num": "2", "account": "188"}, {"name": "田明艳", "num": "3", "account": "187"},
                           {"name": "陈婧婧", "num": "3", "account": "186"}, {"name": "周正", "num": "2", "account": "180"},
                           {"name": "叶鑫", "num": "2", "account": "180"}, {"name": "汪娇", "num": "2", "account": "180"},
                           {"name": "江新英", "num": "2", "account": "152"}, {"name": "朱海波", "num": "2", "account": "150"},
                           {"name": "许鸿", "num": "2", "account": "150"}, {"name": "谢奇敏", "num": "2", "account": "150"},
                           {"name": "徐晓琛", "num": "2", "account": "150"}, {"name": "蒋悠然", "num": "2", "account": "150"},
                           {"name": "钟思琪", "num": "2", "account": "150"}, {"name": "冯晶晶", "num": "2", "account": "150"},
                           {"name": "徐嘉威", "num": "2", "account": "135"}, {"name": "朱瑞", "num": "2", "account": "135"},
                           {"name": "刘进进", "num": "2", "account": "132"}, {"name": "巫强", "num": "2", "account": "132"},
                           {"name": "丁晶晶", "num": "2", "account": "132"}, {"name": "邹毅", "num": "2", "account": "132"},
                           {"name": "章红霞", "num": "2", "account": "132"}, {"name": "孙燕", "num": "2", "account": "132"},
                           {"name": "杨嘉琳", "num": "2", "account": "129"}, {"name": "张珍妮", "num": "1", "account": "128"},
                           {"name": "戴妮", "num": "2", "account": "126"}, {"name": "邵银凡", "num": "2", "account": "120"},
                           {"name": "孙莉莉", "num": "2", "account": "120"}, {"name": "张青海", "num": "2", "account": "118"},
                           {"name": "顾莹莹", "num": "2", "account": "118"}, {"name": "高兴", "num": "1", "account": "90"},
                           {"name": "冯立华", "num": "1", "account": "90"}, {"name": "陈立彦", "num": "1", "account": "90"},
                           {"name": "胡燕", "num": "1", "account": "90"}, {"name": "华雯琦", "num": "1", "account": "90"},
                           {"name": "蔡佳玲", "num": "1", "account": "90"}, {"name": "冯依君", "num": "1", "account": "90"},
                           {"name": "吴珊凤", "num": "1", "account": "69"}, {"name": "李双姣", "num": "1", "account": "69"},
                           {"name": "叶佩恬", "num": "1", "account": "69"}, {"name": "郑蒙燕", "num": "1", "account": "69"},
                           {"name": "娄优琴", "num": "1", "account": "69"}, {"name": "林雅婷", "num": "1", "account": "69"},
                           {"name": "戎旭栋", "num": "1", "account": "69"}, {"name": "吴文燕", "num": "1", "account": "69"},
                           {"name": "殷英", "num": "1", "account": "66"}, {"name": "李娜", "num": "1", "account": "66"},
                           {"name": "李颖烨", "num": "1", "account": "66"}, {"name": "陆涵之", "num": "1", "account": "66"},
                           {"name": "金静珧", "num": "1", "account": "66"}, {"name": "竺京", "num": "1", "account": "66"},
                           {"name": "王栋", "num": "1", "account": "66"}, {"name": "徐文波", "num": "1", "account": "66"},
                           {"name": "刘燕琦", "num": "1", "account": "66"}, {"name": "木梦瑶", "num": "1", "account": "66"},
                           {"name": "杜萍", "num": "1", "account": "66"}, {"name": "张立科", "num": "1", "account": "66"},
                           {"name": "赵颖莹", "num": "1", "account": "66"}, {"name": "杨亚茜", "num": "1", "account": "66"},
                           {"name": "张双双", "num": "1", "account": "255"}, {"name": "黄玲飞", "num": "1", "account": "66"},
                           {"name": "张闪闪", "num": "1", "account": "60"}, {"name": "陶明", "num": "1", "account": "60"},
                           {"name": "陈慈", "num": "0", "account": "0"}, {"name": "杨剑", "num": "0", "account": "0"},
                           {"name": "李若瑜", "num": "0", "account": "373"}]

        for i in range(len(json_before_217)):
            one = json_before_217[i]
            self.total_account_object_before_217[str(one['name'])] = {
                'account': float(one['account']),
                'num': float(one['num']),
            }

    def cal_ts_cumulative_refunded(self):
        """
        获取 天时同城系统 累积营业额(含退款)
        :return:
        """
        condition = {
            'DelStatus': 0,
            "PayStatus": 2,
            "chan3_pin3_ID": {"$ne": "4056068"},
            'OrderTimeISODate': {
                # '$gte': cst_tz.localize(datetime.datetime(2020, 2, 22, 17, 0, 0)),
                '$lt': self.OrderTimeISODate_end,
            },
        }

        group = {
            "_id": "$BelongName",
            "AccountIncludeRefunded": {"$sum": '$ling2_shou4_zong3_jin1_e2'},
        }

        res = self.col_NNWOrder.aggregate(
            [
                {'$match': condition},
                {'$group': group},
            ]
        )

        return_list = []

        for one in res:
            return_list.append(one)

        for i in range(len(return_list)):
            one = return_list[i]
            self.total_account_object_i_r[str(one['_id'])] = {
                'AccountIncludeRefunded': float(one['AccountIncludeRefunded']),
            }

    def cal_ts_cumulative(self):
        """
        获取 天时同城系统 累积营业额
        :return:
        """
        condition = {
            'DelStatus': 0,
            "PayStatus": 2,
            "chan3_pin3_ID": {"$ne": "4056068"},
            "SalesVolume": {"$gt": 0},
            'OrderTimeISODate': {
                '$gte': self.OrderTimeISODate_begin_total,
                '$lt': self.OrderTimeISODate_end_total,
            },
        }

        group = {
            "_id": "$BelongName",
            "order_num": {"$sum": 1},
            "SalesVolume": {"$sum": '$SalesVolume'},
            "BusinessTurnover": {"$sum": '$BusinessTurnover'},
            "BelongGroup": {"$first": "$BelongGroup"},
            "BelongGroupId": {"$first": "$BelongGroupId"},
            "BelongGroupClass": {"$first": "$BelongGroupClass"},
        }

        res = self.col_NNWOrder.aggregate(
            [
                {'$match': condition},
                {'$group': group},
                # {'$project': temp_project},
                # unwind,
            ]
        )

        return_list = []

        for one in res:
            return_list.append(one)

        for i in range(len(return_list)):
            one = return_list[i]
            self.total_account_object[str(one['_id'])] = {
                'BusinessTurnover': float(one['BusinessTurnover']),
                'SalesVolume': float(one['SalesVolume']),
                'OrderNum': float(one['order_num']),
            }

    def cal_ts_cumulative_newGroup(self):
        """
        获取 天时同城系统 累积营业额
        :return:
        """
        condition = {
            'DelStatus': 0,
            "PayStatus": 2,
            "chan3_pin3_ID": {"$ne": "4056068"},
            "SalesVolume": {"$gt": 0},
            'OrderTimeISODate': {
                '$gte': self.OrderTimeISODate_begin_total_newGroup,
                '$lt': self.OrderTimeISODate_end_total,
            },
        }

        group = {
            "_id": "$BelongName",
            "order_num": {"$sum": 1},
            "SalesVolume": {"$sum": '$SalesVolume'},
            "BusinessTurnover": {"$sum": '$BusinessTurnover'},
            "BelongGroup": {"$first": "$BelongGroup"},
            "BelongGroupId": {"$first": "$BelongGroupId"},
            "BelongGroupClass": {"$first": "$BelongGroupClass"},
        }

        res = self.col_NNWOrder.aggregate(
            [
                {'$match': condition},
                {'$group': group},
                # {'$project': temp_project},
                # unwind,
            ]
        )

        return_list = []

        for one in res:
            return_list.append(one)

        for i in range(len(return_list)):
            one = return_list[i]
            self.total_account_object_newGroup[str(one['_id'])] = {
                'BusinessTurnover': float(one['BusinessTurnover']),
                'SalesVolume': float(one['SalesVolume']),
                'OrderNum': float(one['order_num']),
            }

    def update_group_id(self):
        temp_condition = {
            "DelStatus": 0,
        }

        res = self.col_NNWGroup.find(temp_condition)

        group_object = {}
        for one in res:
            group_object[str(one['GroupName'])] = one

        temp_condition = {
            'BelongGroup': {'$exists': True},
            'BelongGroupId': {'$exists': False},
        }

        temp_project = {
            '_id': 1,
            'BelongGroup': 1,
        }

        res = self.col_NNWOrder.find(temp_condition, temp_project)
        bulk_op = []

        for one in res:
            if not one['BelongGroup']:
                continue

            temp_one = UpdateOne(
                {
                    '_id': one['_id'],
                },
                {
                    '$set': {
                        'BelongGroupId': group_object[str(one['BelongGroup'])]['GroupId'],
                        'BelongGroupClass': group_object[str(one['BelongGroup'])]['GroupClass'],
                    },
                    "$unset": {"GroupId": 1, "GroupClass": 1}
                },
            )
            bulk_op.append(temp_one)

        if len(bulk_op) > 0:
            self.col_NNWOrder.bulk_write(bulk_op)

    def transfer_belong(self):
        temp_condition = {
            'BelongName': '刘洁',
            'BelongGroup': '客群组',
        }

        self.col_NNWOrder.update_many(
            temp_condition,
            {
                "$set": {
                    'BelongName': '刘丽洁',
                    'BelongGroup': '产品一组',
                    'TransferLog': [
                        {
                            "BelongName": "刘洁",
                            "BelongId": "29322572",
                            "BelongGroup": "客群组"
                        }
                    ]
                }
            },
        )

    def update_BusinessTurnover(self):
        # 更新NNWOrder 表 营业额
        temp_condition = {
            'DelStatus': 0,
        }

        temp_project = {
            'SalesVolume': 1,
            'ling2_shou4_jia4_ge2': 1,
        }

        res = self.col_NNWOrder.find(temp_condition, temp_project)
        bulk_op = []

        for one in res:
            temp_data = {}
            temp_data['BusinessTurnover'] = float(0)
            try:
                temp_data['BusinessTurnover'] += float(one['SalesVolume'] * one['ling2_shou4_jia4_ge2'])
            except:
                pass

            temp_one = UpdateOne(
                {
                    '_id': one['_id'],
                },
                {
                    '$set': {
                        'BusinessTurnover': float(temp_data['BusinessTurnover'])
                    }
                },
            )

            bulk_op.append(temp_one)

        if len(bulk_op) > 0:
            self.col_NNWOrder.bulk_write(bulk_op)

    def update_BusinessTurnoverCost(self):
        # 更新NNWOrder 表 营业额
        temp_condition = {
            'DelStatus': 0,
        }

        temp_project = {
            'SalesVolume': 1,
            'cai3_gou4_jia4': 1,
        }

        res = self.col_NNWOrder.find(temp_condition, temp_project)
        bulk_op = []

        for one in res:
            temp_data = {}
            temp_data['BusinessTurnoverCost'] = float(0)
            try:
                temp_data['BusinessTurnoverCost'] += float(one['SalesVolume'] * one['cai3_gou4_jia4'])
            except:
                pass

            temp_one = UpdateOne(
                {
                    '_id': one['_id'],
                },
                {
                    '$set': {
                        'BusinessTurnoverCost': float(temp_data['BusinessTurnoverCost'])
                    }
                },
            )

            bulk_op.append(temp_one)

        if len(bulk_op) > 0:
            self.col_NNWOrder.bulk_write(bulk_op)

    def update_GrossProfit(self):
        # 更新NNWOrder 表 毛利
        temp_condition = {
            'DelStatus': 0,
        }

        temp_project = {
            'BusinessTurnover': 1,
            'BusinessTurnoverCost': 1,
        }

        res = self.col_NNWOrder.find(temp_condition, temp_project)
        bulk_op = []

        for one in res:
            temp_data = {}
            temp_data['GrossProfit'] = float(0)
            try:
                temp_data['GrossProfit'] += float(one['BusinessTurnover'] - one['BusinessTurnoverCost'])
            except:
                pass

            temp_one = UpdateOne(
                {
                    '_id': one['_id'],
                },
                {
                    '$set': {
                        'GrossProfit': float(temp_data['GrossProfit'])
                    }
                },
            )

            bulk_op.append(temp_one)

        if len(bulk_op) > 0:
            self.col_NNWOrder.bulk_write(bulk_op)

    def update_SalesVolume(self):
        # 更新NNWOrder 表 销量
        temp_condition = {
            'DelStatus': 0,
        }

        temp_project = {
            'ke3_shi3_yong4_shu4': 1,
            'yi3_shi3_yong4_shu4': 1,
            'yi3_tui4_dan1_shu4': 1,
            'zong3_shu4': 1,
            'ling2_shou4_jia4_ge2': 1,
            'cai3_gou4_jia4': 1,
        }

        res = self.col_NNWOrder.find(temp_condition, temp_project)
        bulk_op = []

        for one in res:
            temp_data = {}
            temp_data['SalesVolume'] = 0
            try:
                temp_data['SalesVolume'] = int(one['zong3_shu4']) - int(one['yi3_tui4_dan1_shu4'])
            except:
                pass

            temp_data['BusinessTurnover'] = float(temp_data['SalesVolume'] * one['ling2_shou4_jia4_ge2'])
            temp_data['BusinessTurnoverCost'] = float(temp_data['SalesVolume'] * one['cai3_gou4_jia4'])
            temp_data['GrossProfit'] = temp_data['BusinessTurnover'] - temp_data['BusinessTurnoverCost']

            temp_one = UpdateOne(
                {
                    '_id': one['_id'],
                },
                {
                    '$set': temp_data
                },
            )

            bulk_op.append(temp_one)
            if len(bulk_op) > 1000:
                self.col_NNWOrder.bulk_write(bulk_op)
                bulk_op = []

        if len(bulk_op) > 0:
            self.col_NNWOrder.bulk_write(bulk_op)

    def query_order(self):
        temp_condition = {
            'DelStatus': 0,
        }

        temp_project = {
            'ID': 1,
        }

        res = self.col_NNWOrder.find(temp_condition, temp_project)

        return res

    def import_superior_id(self):
        json = [{"ID":"29408675","上级ID":"1648629"}]
        bulk_op = []
        for one in json:
            temp_one = UpdateOne(
                {
                    'DelStatus': 0,
                    'ID': str(one['ID']),
                },
                {
                    '$set': {
                        'shang4_ji2_ID': str(one['上级ID'])
                    }
                },
                True
            )
            bulk_op.append(temp_one)

        if len(bulk_op) > 0:
            self.col_NNWCustomer.bulk_write(bulk_op)

    def import_order_customer_id(self):
        # 将下单人id更新到订单中
        json = [{"oid":"232578995","cid":"1648590"}]

        order_list = self.query_order()
        order_object = {}
        bulk_op = []
        
        for one in order_list:
            temp_index = str(one['ID'])
            order_object[temp_index] = one

        for one in json:
            if str(one['oid']) in order_object:
                temp_one = UpdateOne(
                    {
                        'DelStatus': 0,
                        '_id': order_object[str(one['oid'])]['_id'],
                    },
                    {
                        '$set': {
                            'OrderCustomerId': str(one['cid'])
                        }
                    },
                )
                bulk_op.append(temp_one)

        if len(bulk_op) > 0:
            self.col_NNWOrder.bulk_write(bulk_op)

    def import_excel_two(self):
        # 用有收货地址那份excel导入

        print('start import_excel_two')
        res = self.read_excel()
        column = res.pop(0)

        bulk_op = []

        for one in res:
            insert_data = self.set_insert_data_two(one, column)

            temp_one = UpdateOne(
                {
                    'DelStatus': 0,
                    'ID': str(insert_data['ID']),
                },
                {
                    '$set': insert_data
                },
                True,
            )
            bulk_op.append(temp_one)

        if len(bulk_op) > 0:
            self.col_NNWOrder.bulk_write(bulk_op)

    def set_insert_data_two(self, data, column):
        # 处理有收获地址

        param = {}
        param[column[0]] = str(int(float(data[0])))
        param[column[1]] = str(int(float(data[1])))
        param[column[2]] = str(data[2])
        param[column[3]] = str(data[3])
        param[column[4]] = str(data[4])
        param[column[5]] = str(int(float(data[5])))  # 产品ID
        param[column[6]] = str(data[6])
        param[column[7]] = str(data[7])
        param[column[8]] = str(data[8])
        param[column[9]] = str(data[9])
        param[column[10]] = str(data[10])  # 每份返现金额
        param[column[11]] = str(data[11])
        param[column[12]] = str(data[12])

        i = 12
        i += 1
        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(data[i])  # 零售总利润
        i += 1

        param[column[i]] = str(int(float(data[i])))
        i += 1

        param[column[i]] = str(int(float(data[i])))
        i += 1

        param[column[i]] = str(int(float(data[i])))
        i += 1

        param[column[i]] = str(int(float(data[i])))
        i += 1

        param[column[i]] = str(int(float(data[i])))  # 已退单数
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(int(float(data[i]))) if data[i] else ''  # 用户ID
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(int(float(data[i]))) if data[i] else ''  # 上级用户ID
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(int(float(data[i]))) if data[i] else ''
        i += 1

        param[column[i]] = str(data[i])  # 操作员ID
        i += 1

        param[column[i]] = str(int(float(data[i]))) if data[i] else ''
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(int(float(data[i]))) if data[i] else ''
        i += 1

        param[column[i]] = str(int(float(data[i]))) if data[i] else ''  # 联系人身份证号
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(int(float(data[i]))) if data[i] else ''
        i += 1

        param[column[i]] = str(int(float(data[i]))) if data[i] else ''
        i += 1

        param[column[i]] = self.get_time(data[i], 1)
        i += 1

        param[column[i]] = self.get_time(data[i], 1)  # 开始日期
        i += 1

        param[column[i]] = self.get_time(data[i], 1)
        i += 1

        param[column[i]] = self.get_time(data[i], 1)
        i += 1

        param[column[i]] = self.get_time(data[i], 1)
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(data[i])  # 座位号
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(int(float(data[i]))) if data[i] else ''
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = self.get_time(data[i], 1)  # 验证时间
        i += 1

        param[column[i]] = self.get_time(data[i], 1)
        i += 1

        param[column[i]] = self.get_time(data[i], 1)
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(int(float(data[i]))) if data[i] else ''  # 支付流水号
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(int(float(data[i]))) if data[i] else ''
        i += 1

        param[column[i]] = str(data[i])
        i += 1

        param[column[i]] = str(int(float(data[i]))) if data[i] else ''  # 下单人id
        i += 1

        param['订单数'] = '1'
        param['DelStatus'] = 0

        return param

    def import_personnel_two(self):
        # 黄牌
        json = [{"姓名":"郑芬亚","id":"29479430","id2":"","组名":"财务组","南泥湾":"","职级":"C1","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"王英","id":"1648989","id2":"","组名":"财务组","南泥湾":"","职级":"C2","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"张奕红","id":"1664478","id2":"","组名":"财务组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"李琼瑶","id":"1648641","id2":"","组名":"财务组","南泥湾":"","职级":"C1","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"代凤","id":"29214236","id2":"","组名":"财务组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"王良飞","id":"29232824","id2":"","组名":"门店组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"李秀贵","id":"1664337","id2":"","组名":"信息组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"耿巍嵩","id":"1664310","id2":"","组名":"信息组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"朱晓东","id":"29215385","id2":"","组名":"信息组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"陈燕娜","id":"29203346","id2":"","组名":"信息组","南泥湾":"1","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"周科杰","id":"1649748","id2":"","组名":"信息组","南泥湾":"1","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"张滨彬","id":"29093900","id2":"","组名":"信息组","南泥湾":"1","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"夏耀辉","id":"29486354","id2":"","组名":"信息组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"赵启航","id":"29214947","id2":"","组名":"信息组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"李俊杰","id":"29215136","id2":"","组名":"信息组","南泥湾":"1","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"姜晓超","id":"29214827","id2":"","组名":"信息组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"李聪瑜","id":"29215049","id2":"","组名":"信息组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"刘东","id":"29484785","id2":"","组名":"信息组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"1","黄牌次数":"1"},{"姓名":"徐晓琛","id":"29215541","id2":"","组名":"财务组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"张超","id":"29214398","id2":"","组名":"财务组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"张双双","id":"29214206","id2":"","组名":"财务组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"赵颖莹","id":"29214527","id2":"","组名":"财务组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"邵银凡","id":"29214845","id2":"","组名":"财务组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"张立科","id":"29214173","id2":"","组名":"财务组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"傅栋静","id":"29214596","id2":"","组名":"财务组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"毛余雅","id":"29214737","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"李丽梅","id":"1648791","id2":"1648794","组名":"产品二组","南泥湾":"1","职级":"B2","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"徐蕾","id":"29214656","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"章红霞","id":"29214884","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"毛欣璐","id":"29215172","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"叶建峰","id":"1648908","id2":"","组名":"产品二组","南泥湾":"","职级":"C1","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"钟思琪","id":"29214719","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"冯依君","id":"29214683","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"金晓玲","id":"29215148","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"苗婧婧","id":"29214410","id2":"","组名":"产品二组","南泥湾":"","职级":"C1","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"盛小楠","id":"29215505","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"郑蒙燕","id":"29215445","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"陆涵之","id":"29214422","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"汪娇","id":"29215784","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"陈锡锋","id":"1648626","id2":"","组名":"产品二组","南泥湾":"","职级":"C3","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"周晓婉","id":"29214326","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"沈洁","id":"29214416","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"徐嘉威","id":"1649031","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"徐文波","id":"29215001","id2":"","组名":"产品二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"周挺","id":"29214464","id2":"","组名":"产品二组","南泥湾":"","职级":"C2","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"夏至诚","id":"29214428","id2":"","组名":"产品二组","南泥湾":"","职级":"C3","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"俞文文","id":"29214389","id2":"","组名":"产品三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吴毓翔","id":"29214761","id2":"","组名":"产品三组","南泥湾":"","职级":"B1","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"朱晓丽","id":"1649583","id2":"","组名":"产品三组","南泥湾":"","职级":"C3","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"朱冰蕊","id":"1649625","id2":"","组名":"产品三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"竺京","id":"29218148","id2":"","组名":"产品三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"张越","id":"29214491","id2":"","组名":"产品三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"肖艺","id":"29284868","id2":"","组名":"产品三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"徐梦威","id":"29214392","id2":"","组名":"产品三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吴烨运","id":"9373950","id2":"","组名":"产品三组","南泥湾":"","职级":"C2","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"乐林乐","id":"1656408","id2":"","组名":"产品三组","南泥湾":"","职级":"C3","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"叶巧丹","id":"29214275","id2":"","组名":"产品三组","南泥湾":"","职级":"C3","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"程琳娜","id":"29214224","id2":"","组名":"产品三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"符玲霞","id":"1654233","id2":"","组名":"产品三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"徐升","id":"1339035","id2":"","组名":"产品三组","南泥湾":"","职级":"B2","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"周黎民","id":"29214554","id2":"","组名":"产品一组","南泥湾":"1","职级":"B2","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"尤美丽","id":"9098364","id2":"","组名":"产品一组","南泥湾":"","职级":"C2","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"卓碧雅","id":"29214581","id2":"","组名":"产品一组","南泥湾":"","职级":"C2","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"周丽萍","id":"1649217","id2":"","组名":"产品一组","南泥湾":"","职级":"C2","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王亚琴","id":"29214905","id2":"","组名":"产品一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈瑶","id":"1649085","id2":"","组名":"产品一组","南泥湾":"","职级":"C1","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"李敏","id":"29214461","id2":"","组名":"产品一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"谢安娜","id":"1649232","id2":"","组名":"产品一组","南泥湾":"","职级":"C3","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"冯杨","id":"1648593","id2":"","组名":"产品一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"胡丽清","id":"1648656","id2":"","组名":"产品一组","南泥湾":"","职级":"C2","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"朱希","id":"29214170","id2":"","组名":"产品一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"谢奇敏","id":"1649124","id2":"","组名":"产品一组","南泥湾":"","职级":"C1","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"陈丽娜","id":"29214983","id2":"","组名":"产品一组","南泥湾":"","职级":"B1","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"张蓓蓓","id":"29214209","id2":"","组名":"产品一组","南泥湾":"","职级":"C3","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"沈婷婷","id":"29215025","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"周佳艳","id":"1648608","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"任墺娜","id":"29215325","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈建栋","id":"29215070","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"陈亚巧","id":"29214848","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"吴梦梦","id":"29214992","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"田明艳","id":"29214902","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"叶佳丽","id":"29214371","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王志豪","id":"29215256","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"胡伟雄","id":"29215469","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"干慎跃","id":"29214425","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"金静珧","id":"29215274","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"娄优琴","id":"29214587","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"蔡佳玲","id":"29215298","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"朱长江","id":"29214542","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"郑笛","id":"29214221","id2":"","组名":"导服二组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吴敏","id":"29216609","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"华梦阳","id":"29215949","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"杜欣毅","id":"29216414","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"石维","id":"29216015","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"周炜","id":"29216075","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"金玲玲","id":"1648605","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"严梦莹","id":"29215940","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"张灵佳","id":"29214323","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"宋仙红","id":"29214254","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"孙益","id":"1648728","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"许一金","id":"29214260","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"刘燕琦","id":"29215814","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"李双姣","id":"2050194","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"徐家平","id":"29215280","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"钱咪咪","id":"29215994","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"谢晓丹","id":"29214152","id2":"","组名":"导服三组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"李若瑜","id":"29214272","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"许佳玲","id":"2053128","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陆妃娅","id":"29214548","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈立","id":"29214623","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"张立峰","id":"29214620","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"孙莉莉","id":"2059965","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"张珍妮","id":"29215301","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"张旭光","id":"29219669","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"潘施克","id":"29214665","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"陶明","id":"29215673","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"陈云云","id":"29216276","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"毛慧娜","id":"29214830","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"林雅婷","id":"29215124","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"毛乾勋","id":"29214899","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"庄艳","id":"29214200","id2":"","组名":"导服一组","南泥湾":"","职级":"C1","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"徐丽娜","id":"29214671","id2":"","组名":"导服一组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"刘丽洁","id":"29214704","id2":"29322572","组名":"客群组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"高兴","id":"29225603","id2":"","组名":"客群组","南泥湾":"1","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"柳杰","id":"1648920","id2":"","组名":"客群组","南泥湾":"","职级":"C1","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"郑栋豪","id":"29222366","id2":"","组名":"客群组","南泥湾":"1","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"邹毅","id":"2549544","id2":"","组名":"客群组","南泥湾":"1","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"方烨","id":"29215907","id2":"","组名":"客群组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"钱凯怡","id":"29216420","id2":"","组名":"客群组","南泥湾":"1","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"吴灵紫","id":"21808377","id2":"","组名":"客群组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"邵飞国","id":"1303716","id2":"","组名":"客群组","南泥湾":"1","职级":"C2","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"顾莹莹","id":"3416067","id2":"","组名":"客群组","南泥湾":"1","职级":"C3","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"叶鑫","id":"13259319","id2":"","组名":"客群组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"罗曼青","id":"1687299","id2":"","组名":"客群组","南泥湾":"1","职级":"C2","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"李晓俊","id":"1649136","id2":"","组名":"客群组","南泥湾":"1","职级":"C1","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"奚挺","id":"1649052","id2":"","组名":"客群组","南泥湾":"","职级":"B2","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"徐银珠","id":"29215796","id2":"","组名":"客群组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"黄珏","id":"29215277","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王明明","id":"29231633","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"巫强","id":"29273006","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王辰","id":"29234669","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"金伟峰","id":"29361335","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"金建芳","id":"29272538","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"邵丹","id":"29233064","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"张闪闪","id":"29326628","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"冯晶晶","id":"29273054","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"邓阿红","id":"29329847","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"郭燕","id":"29274320","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吴均丹","id":"29336603","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"余兴凯","id":"29216219","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"何赛月","id":"29227349","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"毛淑英","id":"29227835","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吴碧霞","id":"29227001","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"秦燕","id":"29232548","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"许鸿","id":"29227460","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"朱荧洁","id":"29227139","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"谢晴雯","id":"29226983","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"何莲","id":"29227304","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"汪益维","id":"29227199","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"唐梅","id":"29227748","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吴珊凤","id":"29227778","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"薛梅杰","id":"29227631","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈鑫","id":"29227427","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"杨小芳","id":"29227088","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"谢东山","id":"29214437","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"刘进进","id":"29325506","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"丁晶晶","id":"29325464","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吴江琴","id":"29325509","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"章雅璐","id":"29325680","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"孙燕","id":"29325395","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"曹庆利","id":"29325653","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"敖玙璠","id":"29325812","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"张凯","id":"29325827","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈琳红","id":"29325914","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王艺","id":"29325887","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"殷英","id":"29325785","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"金烔","id":"29325839","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"沈亚军","id":"29325956","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"金凯迪","id":"29326061","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王洁","id":"29326415","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"徐凯伦","id":"29326328","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"谢汤涵","id":"29326916","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"李娜","id":"29327147","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"曹芳青","id":"29326403","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"杨璇杰","id":"29327828","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"谢昀庭","id":"29328326","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"高莎莎","id":"29328386","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"孙梁","id":"29329166","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈玉卡","id":"29325611","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"洪雯雯","id":"29326478","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王萍萍","id":"29332652","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"黄玲飞","id":"29231486","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈莉雯","id":"29216879","id2":"","组名":"联创杭州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"杨嘉琳","id":"29214251","id2":"","组名":"联创上海","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"姚志文","id":"29226464","id2":"","组名":"联创上海","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"董林敏","id":"29226821","id2":"","组名":"联创上海","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"郭蓓蓓","id":"29215130","id2":"","组名":"联创上海","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"阮博逊","id":"29226926","id2":"","组名":"联创上海","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"史悦","id":"29227082","id2":"","组名":"联创上海","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王纯侠","id":"0","id2":"","组名":"联创上海","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"冯利华","id":"29214404","id2":"","组名":"联创上海","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"祝莉娜","id":"29330165","id2":"","组名":"联创苏州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"方琪","id":"29331896","id2":"","组名":"联创苏州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王利","id":"29215616","id2":"","组名":"联创苏州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陆莹","id":"29274560","id2":"","组名":"联创苏州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"付茫","id":"29275619","id2":"","组名":"联创苏州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王江丽","id":"29274755","id2":"","组名":"联创苏州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王余杰","id":"29274635","id2":"","组名":"联创苏州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"李贤昱","id":"29215859","id2":"","组名":"联创苏州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"姜凡一","id":"29274608","id2":"","组名":"联创苏州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"颜睿","id":"29216093","id2":"","组名":"联创苏州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"洪莹","id":"29215886","id2":"","组名":"联创苏州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"朱瑞","id":"29216039","id2":"","组名":"联创苏州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"魏强","id":"29215787","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"苏婵","id":"29214503","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"林芝","id":"29214239","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"曾佳雯","id":"29215046","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"饶欢欢","id":"29215556","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"谢淑芳","id":"29215535","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"木梦瑶","id":"29214512","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"金晓丽","id":"29215073","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吴丰","id":"29215196","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈晓春","id":"29214770","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈慈","id":"29215094","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"柯园园","id":"29215100","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吕燕华","id":"29214749","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"夏琼","id":"29215667","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"蒋悠然","id":"29215016","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"金伟健","id":"29215112","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"曹皓程","id":"29215253","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"徐旭","id":"1649325","id2":"","组名":"联创温州","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"沈颖","id":"29215397","id2":"","组名":"门店组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈周舟","id":"29215340","id2":"","组名":"门店组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"胡春燕","id":"29214248","id2":"","组名":"门店组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"董沁沁","id":"29215793","id2":"","组名":"门店组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"赵辉","id":"29216084","id2":"","组名":"门店组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"梅晶晶","id":"29214851","id2":"","组名":"门店组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"李宁","id":"29216126","id2":"","组名":"门店组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"周惠叶","id":"3446868","id2":"","组名":"门店组","南泥湾":"","职级":"C2","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"周维","id":"2050965","id2":"","组名":"门店组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"杨益娜","id":"29214557","id2":"","组名":"门店组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"华英达","id":"29214533","id2":"","组名":"门店组","南泥湾":"","职级":"C2","大组":"私顾组","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"胡丹珠","id":"1648821","id2":"","组名":"门店组","南泥湾":"","职级":"C3","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"卢丽娜","id":"29214341","id2":"","组名":"门店组","南泥湾":"","职级":"C2","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"金波","id":"29496911","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"郑赵磊","id":"29498150","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"马依儿","id":"29502275","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"徐丽红","id":"29461685","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"金晓","id":"29495951","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王华香","id":"29438702","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"易园","id":"29498303","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"孔令宇","id":"29495261","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"朱晓静","id":"29498651","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"蔡俊杰","id":"29321771","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王静静","id":"29500025","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"袁超","id":"29501069","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"夏科强","id":"3431283","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"林送","id":"29339561","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"张洁昕","id":"1648650","id2":"","组名":"启航组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吴慧雯","id":"1664331","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"周正","id":"29214836","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"高文斌","id":"3445548","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"华雯琦","id":"2072985","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"吴建芸","id":"3245553","id2":"","组名":"签证组","南泥湾":"1","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"杨亚茜","id":"29099675","id2":"","组名":"签证组","南泥湾":"1","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"杨雅竹","id":"2388456","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"姚彦","id":"29215757","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"胡徐灵","id":"29215244","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"吴文燕","id":"2050389","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"胡燕","id":"29215142","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"潘碧雯","id":"29218190","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"江新英","id":"29215391","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"林靖","id":"10912230","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"柴祥华","id":"10911705","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"管雨含","id":"29214446","id2":"","组名":"签证组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"陈健","id":"1649016","id2":"","组名":"签证组","南泥湾":"","职级":"C3","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"戚天恩","id":"1649142","id2":"","组名":"签证组","南泥湾":"","职级":"C1","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"杨露卿","id":"29215751","id2":"","组名":"人资后勤","南泥湾":"1","职级":"B1","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"张静娜","id":"3431706","id2":"","组名":"人资后勤","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"俞珊珊","id":"1648899","id2":"","组名":"人资后勤","南泥湾":"","职级":"C1","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈思瑾","id":"29215064","id2":"","组名":"人资后勤","南泥湾":"","职级":"C1","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"麻胜巍","id":"29214347","id2":"","组名":"人资后勤","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"杨剑","id":"29121110","id2":"","组名":"人资后勤","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"陈英","id":"1649055","id2":"","组名":"人资后勤","南泥湾":"","职级":"C2","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"张瑛","id":"29214434","id2":"","组名":"人资后勤","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"李佳路","id":"14913126","id2":"","组名":"人资后勤","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"虞勤玲","id":"29214521","id2":"","组名":"人资后勤","南泥湾":"","职级":"C1","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"顾佳玲","id":"29214407","id2":"","组名":"私顾二组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"周璐露","id":"29214227","id2":"","组名":"私顾二组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"张金君","id":"29214383","id2":"","组名":"私顾二组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"赵静静","id":"29214593","id2":"","组名":"私顾二组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"刘思彤","id":"9098409","id2":"","组名":"私顾二组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈园园","id":"29214632","id2":"","组名":"私顾二组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"毛丽波","id":"29215343","id2":"","组名":"私顾二组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"石茗菲","id":"9103929","id2":"29214296","组名":"私顾二组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"高红菲","id":"3432060","id2":"","组名":"私顾二组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"罗方舟","id":"2426019","id2":"","组名":"私顾二组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"叶佩恬","id":"29214299","id2":"","组名":"私顾二组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"朱金晶","id":"29214197","id2":"","组名":"私顾二组","南泥湾":"1","职级":"B1","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"汪叶群","id":"29214281","id2":"29328803","组名":"私顾二组","南泥湾":"","职级":"C2","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"徐涯丹","id":"29214356","id2":"","组名":"私顾二组","南泥湾":"","职级":"C1","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"鲍玖玲","id":"1648629","id2":"","组名":"私顾三组","南泥湾":"","职级":"C2","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"竺洁莹","id":"29215115","id2":"2436831","组名":"私顾三组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"袁乐","id":"29215187","id2":"1664328","组名":"私顾三组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"周佳贤","id":"29214917","id2":"2491383","组名":"私顾三组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"邓祥鹏","id":"29214839","id2":"","组名":"私顾三组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"李舟瑶","id":"29215250","id2":"","组名":"私顾三组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"张潇健","id":"29214215","id2":"29214155","组名":"私顾三组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"杨嘉帅","id":"29219732","id2":"29320367","组名":"私顾三组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈诗瑜","id":"29216168","id2":"","组名":"私顾三组","南泥湾":"","职级":"C2","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王健","id":"29215121","id2":"","组名":"私顾三组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"林杰","id":"1687380","id2":"","组名":"私顾三组","南泥湾":"","职级":"C1","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"王展轩","id":"29214137","id2":"","组名":"私顾三组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吴鑫","id":"29214134","id2":"","组名":"私顾三组","南泥湾":"","职级":"C2","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈胡叶","id":"17956164","id2":"1648785","组名":"私顾三组","南泥湾":"","职级":"C3","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"顾立波","id":"1750551","id2":"","组名":"私顾一组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"姚婷婷","id":"2426706","id2":"","组名":"私顾一组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"任珮源","id":"3839361","id2":"","组名":"私顾一组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"徐松松","id":"3839364","id2":"","组名":"私顾一组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"金云","id":"29214821","id2":"","组名":"私顾一组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈美娣","id":"29214806","id2":"","组名":"私顾一组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"蔡佳丽","id":"2426037","id2":"","组名":"私顾一组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈文威","id":"9822576","id2":"","组名":"私顾一组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"钟晓蓉","id":"1649310","id2":"","组名":"私顾一组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"章亦莉","id":"29214269","id2":"","组名":"私顾一组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"孙一帅","id":"29214332","id2":"","组名":"私顾一组","南泥湾":"","职级":"","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"章丹丹","id":"29070947","id2":"1648701","组名":"私顾一组","南泥湾":"","职级":"C3","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"林莉","id":"1650339","id2":"","组名":"私顾一组","南泥湾":"","职级":"C2","大组":"私顾组","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"宓方明","id":"29214716","id2":"","组名":"线上组","南泥湾":"1","职级":"B2","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"张鑫吉","id":"29214986","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"严波梅","id":"29215247","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吴慧静","id":"29215349","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"曹晴晴","id":"29215493","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"许钰婷","id":"29215193","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"陈立彦","id":"21906702","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"陈丹","id":"29215154","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"戴妮","id":"29215307","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"王飘","id":"29214563","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吕浩峰","id":"29214572","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"张盛","id":"1648590","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈婧婧","id":"29214608","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"张丽","id":"29215562","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"张栋","id":"29214875","id2":"","组名":"线上组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"薛娇","id":"1648830","id2":"","组名":"线上组","南泥湾":"","职级":"C2","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"李森松","id":"2426391","id2":"","组名":"信息组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"朱海波","id":"29215067","id2":"","组名":"信息组","南泥湾":"1","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"戎旭栋","id":"29215040","id2":"","组名":"信息组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"伍宣宇","id":"29214785","id2":"","组名":"信息组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"虞云祥","id":"29215034","id2":"","组名":"信息组","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"张大益","id":"29322674","id2":"","组名":"总经办","南泥湾":"","职级":"","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"裘郑","id":"29214242","id2":"","组名":"总经办","南泥湾":"1","职级":"A-","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"何斌锋","id":"29216099","id2":"","组名":"总经办","南泥湾":"","职级":"A+","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"陈晓冬","id":"29215835","id2":"","组名":"总经办","南泥湾":"1","职级":"A","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"李达","id":"1650747","id2":"1650744","组名":"总经办","南泥湾":"1","职级":"A-","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"吴滨","id":"1648581","id2":"","组名":"总经办","南泥湾":"","职级":"A-","大组":"","隐藏个人排名":"","黄牌次数":"0"},{"姓名":"张青海","id":"29216591","id2":"","组名":"总经办","南泥湾":"","职级":"A-","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"熊笛","id":"29214824","id2":"","组名":"总经办","南泥湾":"1","职级":"A-","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"黄宇","id":"29123162","id2":"","组名":"总经办","南泥湾":"1","职级":"A-","大组":"","隐藏个人排名":"","黄牌次数":"1"},{"姓名":"余红","id":"1649805","id2":"","组名":"总经办","南泥湾":"","职级":"B1","大组":"","隐藏个人排名":"","黄牌次数":"0"}]
        temp_condition = {
            'DelStatus': 0,
        }

        res = self.col_NNWGroup.find(temp_condition)

        group_object = {}
        for one in res:
            group_object[str(one['GroupName'])] = one

        bulk_op = []

        for i in range(len(json)):
            data = json[i]

            upper_group = '后台组'
            if str(data['组名']) in ['门店组', '私顾一组', '私顾二组', '私顾三组']:
                upper_group = '私顾组'
            if str(data['组名']) in ['导服一组', '导服二组', '导服三组']:
                upper_group = '导服组'
            if str(data['组名']) in ['启航组']:
                upper_group = '启航组'
            if '联创' in str(data['组名']):
                upper_group = '联创组'
            if str(data['姓名']) == '朱金晶':
                upper_group = '后台组'
            if str(data['姓名']) in ['朱晓丽', '刘丽洁', '俞文文']:
                upper_group = '私顾组'

            temp_one = UpdateOne(
                {
                    'ID': str(data['id']),
                },
                {
                    '$set': {
                        'RealName': str(data['姓名']),
                        'Group': str(data['组名']),
                        'UpperGroup': upper_group,
                        'ID': str(data['id']),
                        'Type': 1,
                        'YellowCardTimes': int(data['黄牌次数']),
                        'BelongNNW': 1 if str(data['南泥湾']) == '1' else 2,
                        'HidePersonRank': 1 if str(data['隐藏个人排名']) == '1' else 2,
                        'JobGrade': str(data['职级']),
                        'GroupId': group_object[str(data['组名'])]['GroupId'],
                        'GroupClass': group_object[str(data['组名'])]['GroupClass'],
                        'DelStatus': 0,
                    }
                },
                True,
            )
            bulk_op.append(temp_one)

            if data['id2']:
                temp_one = UpdateOne(
                    {
                        'ID': str(data['id2']),
                    },
                    {
                        '$set': {
                            'RealName': str(data['姓名']),
                            'Group': str(data['组名']),
                            'UpperGroup': upper_group,
                            'ID': str(data['id2']),
                            'Type': 2,
                            'YellowCardTimes': int(data['黄牌次数']),
                            'BelongNNW': 1 if str(data['南泥湾']) == '1' else 2,
                            'HidePersonRank': 1 if str(data['隐藏个人排名']) == '1' else 2,
                            'JobGrade': str(data['职级']),
                            'GroupId': group_object[str(data['组名'])]['GroupId'],
                            'GroupClass': group_object[str(data['组名'])]['GroupClass'],
                            'DelStatus': 0,
                        }
                    },
                    True,
                )
                bulk_op.append(temp_one)

        if len(bulk_op) > 0:
            self.col_NNWIdName.bulk_write(bulk_op)

    def get_id_name(self):
        temp_condition = {
            'DelStatus': 0,
        }

        res = self.col_NNWIdName.find(temp_condition)

        return res

    def export_excel_two(self):
        """
        1.确保志愿表是最新
        """
        old_object = {}
        id_name_list = self.get_id_name()

        for one in id_name_list:
            old_object[str(one['ID'])] = {
                'RealName': str(one['RealName']),
                'ID': str(one['ID']),
                'Group': str(one['Group']),
            }

        self.old_object = old_object

        temp_condition = {
            'DelStatus': 0,
            "PayStatus": 2,
            "chan3_pin3_ID": {"$ne": "4056068"},
            "SalesVolume": {"$gt": 0},
            'OrderTimeISODate': {
                '$gte': self.OrderTimeISODate_begin,
                '$lt': self.OrderTimeISODate_end,
            },
        }

        # temp_condition = {'DelStatus': 0,
        #     "PayStatus": 2,"chan3_pin3_ID" : "4056713","$or":[{"yi3_tui4_dan1_shu4":{"$gt":0}},{"shen1_qing3_tui4_dan1_shu4":{"$gt":0}}]}
        # temp_condition = {
        #     "chan3_pin3_ID": {"$ne": "4056068"},
        #     "DelStatus": 0,
        #     "BelongName": "陈丽娜",
        #     '$or': [
        #         {'shen1_qing3_tui4_dan1_shu4': {'$gt': 0}},
        #         {'yi3_tui4_dan1_shu4': {'$gt': 0}},
        #     ],
        # }

        # 简洁版需要
        project = {"ID":1, "xia4_dan1_shi2_jian1":1, "BelongName":1, "BelongGroup":1, "ming2_cheng1":1, 'SalesVolume':1,
                    "ling2_shou4_jia4_ge2":1, 'cai3_gou4_jia4':1, "ling2_shou4_zong3_jin1_e2":1, 'BusinessTurnover':1,
                   'BusinessTurnoverCost':1, 'GrossProfit':1}

        # temp_condition = {
        #     'DelStatus': 0,
        #     "PayStatus": 2,
        #     "chan3_pin3_ID": {"$ne": "4056068"},
        #     "SalesVolume": {"$gt": 0},
        #     "BelongName" : "何斌锋",
        # }

        res = self.col_NNWOrder.find(temp_condition)

        new_list = []
        for one in res:
            if 'gong1_ying4_shang1_ID' in one:
                if one['gong1_ying4_shang1_ID'] and one['gong1_ying4_shang1_ID'] not in self.supplier:
                    self.supplier.append(one['gong1_ying4_shang1_ID'])

            one['OrderNum'] = 1
            new_list.append(one)

        self.do_export_excel(new_list)
        if datetime.datetime.now().hour == hourEveryDay:
            self.export_dispatch_bill(new_list)

    def deal_sheet_personnel(self, list, group_class=1):
        temp_condition = {
            'DelStatus': 0,
            'Type': 1,
        }

        id_name_list = self.col_NNWIdName.find(temp_condition)

        personnel_object = {}
        for one in id_name_list:
            if one['GroupClass'] != group_class:
                continue
            try:
                BelongGroup = one['VersionTwoGroup']["GroupName"]
            except:
                BelongGroup = one["Group"]
            temp_index = str(one['RealName'])
            personnel_object[temp_index] = {
                'RealName': one['RealName'],
                "BelongGroup":BelongGroup ,
                'Group': one['Group'],
                'GroupId': one['GroupId'],
                'GroupClass': one['GroupClass'],
                'OrderNum': 0,
                'SaleNum': 0,
                'AccountToday': float(0),
                'AccountTotal_newGroup': self.total_account_object_newGroup[temp_index]['BusinessTurnover'] if temp_index in self.total_account_object_newGroup else float(0),
                'OrderNumTotal_newGroup': self.total_account_object_newGroup[temp_index]['OrderNum'] if temp_index in self.total_account_object_newGroup else float(0),
                'SaleNumTotal_newGroup': self.total_account_object_newGroup[temp_index]['SalesVolume'] if temp_index in self.total_account_object_newGroup else float(0),

                'AccountTotal': self.total_account_object[temp_index]['BusinessTurnover'] if temp_index in self.total_account_object else float(0),
                'OrderNumTotal': self.total_account_object[temp_index]['OrderNum'] if temp_index in self.total_account_object else float(0),
                'SaleNumTotal': self.total_account_object[temp_index]['SalesVolume'] if temp_index in self.total_account_object else float(0),
                'AccountIncludeRefunded': self.total_account_object_i_r[temp_index]['AccountIncludeRefunded'] if temp_index in self.total_account_object_i_r else float(0),
                'BelongNNW': one['BelongNNW'],
                'JobGrade': one['JobGrade'],
                'UpperGroup': one['UpperGroup'],
                'HidePersonRank': one['HidePersonRank'],
                'YellowCardTimesCurrent': one['YellowCardTimesCurrent'],
            }

            # 将217 号之前累积营业加上去
            if temp_index in self.total_account_object_before_217:
                personnel_object[temp_index]['AccountTotal'] += self.total_account_object_before_217[temp_index]['account']
                personnel_object[temp_index]['AccountIncludeRefunded'] += self.total_account_object_before_217[temp_index]['account']
                personnel_object[temp_index]['OrderNumTotal'] += self.total_account_object_before_217[temp_index]['num']
                personnel_object[temp_index]['SaleNumTotal'] += self.total_account_object_before_217[temp_index]['num']

        for i in range(len(list)):
            data = list[i]

            if 'BelongId' not in data:
                continue

            if not data['BelongId']:
                continue

            if str(data['BelongName']) in personnel_object:
                personnel_object[str(data['BelongName'])]['OrderNum'] += 1
                personnel_object[str(data['BelongName'])]['SaleNum'] += data['SalesVolume']
                personnel_object[str(data['BelongName'])]['AccountToday'] += data['BusinessTurnover']

        personnel_list = []
        for d in personnel_object:
            if 'GroupId' in personnel_object[d]:
                del personnel_object[d]['GroupId']

            if 'GroupClass' in personnel_object[d]:
                del personnel_object[d]['GroupClass']
            personnel_list.append(personnel_object[d])

        # 排序
        res = sorted(personnel_list, key=operator.itemgetter('AccountTotal'), reverse=True)

        return res

    def export_dispatch_bill(self, data_list):
        """
        导出发货单
        :param data_list:
        :return:
        """
        dir_path = '飞扬发货单' + str(self.year) + '年' + str(self.month) + '月' + str(self.day) + '日'
        os.mkdir(dir_path)

        for s in self.supplier:
            workbook_dispatch_bill = xlwt.Workbook(encoding='utf-8')

            xlwt.add_palette_colour("custom_green_colour", 0x21)
            workbook_dispatch_bill.set_colour_RGB(0x21, 169, 208, 142)

            self.style_column_bkg = xlwt.easyxf(
                'pattern: pattern solid, fore_colour custom_green_colour; font: bold on;')

            temp_data = []
            for order in data_list:
                if order['shen1_qing3_tui4_dan1_shu4'] > 0:
                    continue
                # if order['gong1_ying4_shang1_ID'] == s:
                #     try:
                #         order['ProvinceAddress'] = order['kuai4_di4_di4_zhi3'].split(' ', 3)[0]
                #         order['CityAddress'] = order['kuai4_di4_di4_zhi3'].split(' ', 3)[1]
                #         order['DetailAddress'] = order['kuai4_di4_di4_zhi3'].split(' ', 3)[2]
                #     except:
                #         order['ProvinceAddress'] = ''
                #         order['CityAddress'] = ''
                #         order['DetailAddress'] = ''
                if order['gong1_ying4_shang1_ID'] == s:
                    try:
                        order['ProvinceAddress'] = order['kuai4_di4_di4_zhi3'].split(' ', 3)[0]
                        order['CityAddress'] = order['kuai4_di4_di4_zhi3'].split(' ', 3)[1]
                        order['DetailAddress'] = order['kuai4_di4_di4_zhi3'].split(' ', 3)[2]
                    except:
                        order['ProvinceAddress'] = ''
                        order['CityAddress'] = ''
                        order['DetailAddress'] = ''
                    temp_data.append(order)

            sheet_name = temp_data[0]['gong1_ying4_shang1']

            excel_name = dir_path + '/' + sheet_name + '.xls'
            worksheet = workbook_dispatch_bill.add_sheet(sheet_name)
            worksheet = self.create_dispatch_bill_excel(temp_data, worksheet)

            # 保存文件
            workbook_dispatch_bill.save(excel_name)

    def create_dispatch_bill_excel(self, data_list, worksheet):
        """
        创建发货单excel
        :param data_list:
        :param worksheet:
        :return:
        """
        # 标题处理开始
        column_name = [{"name": "订单ID", "width": 12}, {"name": "产品ID", "width": 12}, {"name": "商品名称", "width": 20},
                       {"name": "详细名称", "width": 10},
                       {"name": "购买数量", "width": 10}, {"name": "收货人姓名", "width": 12}, {"name": "收货人手机", "width": 12},
                       {"name": "备注", "width": 12},
                       {"name": "用户备注", "width": 12},
                        {"name": "下单时间", "width": 20}, {"name": "原快递地址", "width": 20},
                       {"name": "预期发货时间", "width": 20},{"name": "快递公司", "width": 10},
                       {"name": "快递单号", "width": 20}]
        #{"name": "省", "width": 10},"name": "市", "width": 10},{"name": "详细地址", "width": 20},

        i = 0
        for column in column_name:
            worksheet.write(0, i, column['name'], self.style_column_bkg)
            first_col = worksheet.col(i)
            first_col.width = 256 * column['width']
            i += 1
        # 标题处理结束

        # 数据处理开始
        line = 1

        v_column = ["ID", "chan3_pin3_ID", "ming2_cheng1", "SKU_ming2_cheng1", "SalesVolume", "you2_ke4_xing4_ming2",
                    'you2_ke4_shou3_ji1_hao4',
                    'bei4_zhu4', 'yong4_hu4_bei4_zhu4',
                    "xia4_dan1_shi2_jian1", 'kuai4_di4_di4_zhi3',  "yu4_ji4_you2_wan2_shi2_jian1"]
                    #'ProvinceAddress','CityAddress', 'DetailAddress',
        for i in range(len(data_list)):
            data = data_list[i]
            column = 0

            for j in range(len(v_column)):
                try:
                    worksheet.write(line, column, data[v_column[j]])
                except:
                    print(json_util.dumps(data, ensure_ascii=False))
                    exit()
                column += 1

            line += 1
        # 数据处理结束

        return worksheet

    def do_export_excel(self, list):
        # 统计累积营业额
        self.stat_personnel_cumulative_turnover()

        workbook = xlwt.Workbook(encoding='utf-8')

        xlwt.add_palette_colour("custom_green_colour", 0x21)
        workbook.set_colour_RGB(0x21, 169, 208, 142)

        self.style_column_bkg = xlwt.easyxf('pattern: pattern solid, fore_colour custom_green_colour; font: bold on;')

        worksheet = workbook.add_sheet('订单核心统计')
        worksheet = self.do_export_excel_detail_simple(list, worksheet)

        worksheet = workbook.add_sheet('详细导出')
        worksheet = self.do_export_excel_detail_total(list, worksheet)

        # res = self.deal_sheet_goods(list)
        res = self.stat_goods()
        worksheet = workbook.add_sheet('产品统计')
        worksheet = self.do_export_excel_goods(res, worksheet)

        res = self.stat_goods_7()
        worksheet = workbook.add_sheet('近七日产品统计')
        worksheet = self.do_export_excel_goods(res, worksheet)

        res = self.deal_sheet_personnel(list, 1)
        worksheet = workbook.add_sheet('个人统计')
        worksheet = self.do_export_excel_personnel(res, worksheet)

        upper_group = ['私顾组', '后台组', '导服组', '启航组',"轮休组",'WIFI组']
        for i in range(len(upper_group)):
            upper_group_name = upper_group[i]
            sheet_name = upper_group_name + '个人'
            worksheet = workbook.add_sheet(sheet_name)
            worksheet = self.do_export_excel_personnel(res, worksheet, upper_group_name)

        # res_newGroup = self.deal_sheet_personnel(list, 1)
        worksheet = workbook.add_sheet('分组统计')
        res = self.deal_sheet_group(res, 1)
        worksheet = self.do_export_excel_group(res, worksheet)

        res = self.deal_sheet_personnel(list, 2)
        worksheet = workbook.add_sheet('个人统计(联创)')
        worksheet = self.do_export_excel_personnel(res, worksheet)

        worksheet = workbook.add_sheet('分组统计(联创)')
        res = self.deal_sheet_group(res, 2)
        worksheet = self.do_export_excel_group(res, worksheet)

        # 保存文件
        workbook.save('order.xls')

    def stat_goods(self):
        res = self.col_NNWOrder.aggregate(
            [
                {
                    "$match": {
                        "DelStatus": 0,
                        "PayStatus": 2,
                        "chan3_pin3_ID": {"$ne": "4056068"},
                        "SalesVolume": {"$gt": 0},
                        'OrderTimeISODate': {
                            '$gte': self.OrderTimeISODate_begin_total,
                            '$lt': self.OrderTimeISODate_end_total,
                        },
                    }
                },
                {
                    "$group": {
                        "_id": "$chan3_pin3_ID",
                        "order_num_total": {"$sum": 1},
                        "sell_num_total": {"$sum": "$SalesVolume"},
                        "BusinessTurnover_total": {"$sum": "$BusinessTurnover"},
                        "BusinessTurnoverCost_total": {"$sum": "$BusinessTurnoverCost"},
                        "GrossProfit_total": {"$sum": "$GrossProfit"},
                        "name": {"$first": "$ming2_cheng1"},
                        "chan3_pin3_ID": {"$first": "$chan3_pin3_ID"},
                        "skuname": {"$first": "$SKU_ming2_cheng1"}
                    }
                }
            ]
        )

        goods_stat_total_dict = {}
        for one in res:
            goods_id = str(one['chan3_pin3_ID'])
            goods_stat_total_dict[goods_id] = one

            temp_res = self.col_NNWOrder.aggregate(
                [
                    {
                        "$match": {
                            "chan3_pin3_ID": goods_id,
                            "DelStatus": 0,
                            "PayStatus": 2,
                            "SalesVolume": {"$gt": 0},
                            'OrderTimeISODate': {
                                '$gte': self.OrderTimeISODate_begin_total,
                                '$lt': self.OrderTimeISODate_end_total,
                            },
                        }
                    },
                    {
                        "$group": {
                            "_id": "$you2_ke4_shou3_ji1_hao4",
                            "repeat_order_num": {"$sum": 1},
                            "chan3_pin3_ID": {"$first": "$chan3_pin3_ID"},
                        }
                    },
                    {
                        "$match": {
                            "repeat_order_num": {"$gt": 1}
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "repeat_order_num2": {"$sum": "$repeat_order_num"},
                            "repeat_order_num": {"$sum": 1},
                        }
                    },
                ]
            )
            repeat_order_num = 0
            repeat_order_num2 = 0
            for temp_one in temp_res:
                repeat_order_num = temp_one['repeat_order_num']
                repeat_order_num2 = temp_one['repeat_order_num2'] - repeat_order_num

                break

            goods_stat_total_dict[goods_id]['repeat_num'] = repeat_order_num
            goods_stat_total_dict[goods_id]['repeat_order_num2'] = repeat_order_num2

            temp_res = self.col_NNWOrder.aggregate(
                [
                    {
                        "$match": {
                            "chan3_pin3_ID": goods_id,
                            "DelStatus": 0,
                            "PayStatus": 2,
                            "SalesVolume": {"$gt": 0},
                            'OrderTimeISODate': {
                                '$gte': self.OrderTimeISODate_begin_total,
                                '$lt': self.OrderTimeISODate_end_total,
                            },
                        }
                    },
                    {
                        "$group": {
                            "_id": "$you2_ke4_shou3_ji1_hao4",
                            "you2_ke4_shou3_ji1_hao4": {"$sum": 1},
                        },
                    },
                    {
                        "$group": {
                            "_id": None,
                            "total_customer": {"$sum": 1},
                        }
                    },
                ]
            )

            # 下单会员数量
            total_customer = 0
            for temp_one in temp_res:
                total_customer = temp_one['total_customer']
                break

            goods_stat_total_dict[goods_id]['total_customer'] = total_customer
            try:
                goods_stat_total_dict[goods_id]['RepeatOrderRate'] = round(repeat_order_num/total_customer, 4)*100
            except:
                goods_stat_total_dict[goods_id]['RepeatOrderRate'] = 0

            try:
                goods_stat_total_dict[goods_id]['RepeatOrderRate2'] = round(repeat_order_num2 /goods_stat_total_dict[goods_id]['order_num_total'] , 4) * 100
            except:
                goods_stat_total_dict[goods_id]['RepeatOrderRate2'] = 0

            if goods_stat_total_dict[goods_id]['RepeatOrderRate'] == 0:
                goods_stat_total_dict[goods_id]['RepeatOrderRate'] = ''
            if goods_stat_total_dict[goods_id]['RepeatOrderRate2'] == 0:
                goods_stat_total_dict[goods_id]['RepeatOrderRate2'] = ''

        res = self.col_NNWOrder.aggregate(
            [
                {
                    "$match": {
                        "DelStatus": 0,
                        "PayStatus": 2,
                        "chan3_pin3_ID": {"$ne": "4056068"},
                        "SalesVolume": {"$gt": 0},
                        'OrderTimeISODate': {
                            '$gte': self.OrderTimeISODate_begin,
                            '$lt': self.OrderTimeISODate_end,
                        },
                    }
                },
                {
                    "$group": {
                        "_id": "$chan3_pin3_ID",
                        "order_num": {"$sum": 1},
                        "sell_num": {"$sum": "$SalesVolume"},
                        "BusinessTurnover": {"$sum": "$BusinessTurnover"},
                        "BusinessTurnoverCost": {"$sum": "$BusinessTurnoverCost"},
                        "GrossProfit": {"$sum": "$GrossProfit"},
                        "name": {"$first": "$ming2_cheng1"},
                        "chan3_pin3_ID": {"$first": "$chan3_pin3_ID"},
                        "skuname": {"$first": "$SKU_ming2_cheng1"}
                    }
                }
            ]
        )

        goods_dict = {}
        for one in res:
            goods_id = str(one['chan3_pin3_ID'])
            goods_dict[goods_id] = one

            # goods_dict[goods_id]['order_num_total'] = goods_stat_total_dict[goods_id]['order_num_total']
            # goods_dict[goods_id]['sell_num_total'] = goods_stat_total_dict[goods_id]['sell_num_total']
            # goods_dict[goods_id]['BusinessTurnover_total'] = goods_stat_total_dict[goods_id]['BusinessTurnover_total']
            # goods_dict[goods_id]['BusinessTurnoverCost_total'] = goods_stat_total_dict[goods_id]['BusinessTurnoverCost_total']
            # goods_dict[goods_id]['GrossProfit_total'] = goods_stat_total_dict[goods_id]['GrossProfit_total']

        for d in goods_stat_total_dict:
            if d in goods_dict:
                goods_stat_total_dict[d]['order_num'] = goods_dict[d]['order_num']
                goods_stat_total_dict[d]['sell_num'] = goods_dict[d]['sell_num']
                goods_stat_total_dict[d]['BusinessTurnover'] = goods_dict[d]['BusinessTurnover']
                goods_stat_total_dict[d]['BusinessTurnoverCost'] = goods_dict[d]['BusinessTurnoverCost']
                goods_stat_total_dict[d]['GrossProfit'] = goods_dict[d]['GrossProfit']
            else:
                goods_stat_total_dict[d]['order_num'] = 0
                goods_stat_total_dict[d]['sell_num'] = 0
                goods_stat_total_dict[d]['BusinessTurnover'] = 0
                goods_stat_total_dict[d]['BusinessTurnoverCost'] = 0
                goods_stat_total_dict[d]['GrossProfit'] = 0

        goods_list = []
        for d in goods_stat_total_dict:
            goods_list.append(goods_stat_total_dict[d])

        res = sorted(goods_list, key=operator.itemgetter('BusinessTurnover'), reverse=True)

        return res

    def stat_goods_7(self):
        res = self.col_NNWOrder.aggregate(
            [
                {
                    "$match": {
                        "DelStatus": 0,
                        "PayStatus": 2,
                        "chan3_pin3_ID": {"$ne": "4056068"},
                        "SalesVolume": {"$gt": 0},
                        'OrderTimeISODate': {
                            '$gte': self.OrderTimeISODate_begin_7,
                            '$lt': self.OrderTimeISODate_end_total,
                        },
                    }
                },
                {
                    "$group": {
                        "_id": "$chan3_pin3_ID",
                        "order_num_total": {"$sum": 1},
                        "sell_num_total": {"$sum": "$SalesVolume"},
                        "BusinessTurnover_total": {"$sum": "$BusinessTurnover"},
                        "BusinessTurnoverCost_total": {"$sum": "$BusinessTurnoverCost"},
                        "GrossProfit_total": {"$sum": "$GrossProfit"},
                        "name": {"$first": "$ming2_cheng1"},
                        "chan3_pin3_ID": {"$first": "$chan3_pin3_ID"},
                        "skuname": {"$first": "$SKU_ming2_cheng1"}
                    }
                }
            ]
        )

        goods_stat_total_dict = {}
        for one in res:
            goods_id = str(one['chan3_pin3_ID'])
            goods_stat_total_dict[goods_id] = one

            temp_res = self.col_NNWOrder.aggregate(
                [
                    {
                        "$match": {
                            "chan3_pin3_ID": goods_id,
                            "DelStatus": 0,
                            "PayStatus": 2,
                            "SalesVolume": {"$gt": 0},
                            'OrderTimeISODate': {
                                '$gte': self.OrderTimeISODate_begin_7,
                                '$lt': self.OrderTimeISODate_end_total,
                            },
                        }
                    },
                    {
                        "$group": {
                            "_id": "$you2_ke4_shou3_ji1_hao4",
                            "repeat_order_num": {"$sum": 1},
                            "chan3_pin3_ID": {"$first": "$chan3_pin3_ID"},
                        }
                    },
                    {
                        "$match": {
                            "repeat_order_num": {"$gt": 1}
                        }
                    },
                    {
                        "$group": {
                            "_id": None,
                            "repeat_order_num2": {"$sum": "$repeat_order_num"},
                            "repeat_order_num": {"$sum": 1},
                        }
                    },
                ]
            )
            repeat_order_num = 0
            repeat_order_num2 = 0
            for temp_one in temp_res:
                repeat_order_num = temp_one['repeat_order_num']
                repeat_order_num2 = temp_one['repeat_order_num2'] - repeat_order_num

                break

            goods_stat_total_dict[goods_id]['repeat_num'] = repeat_order_num
            goods_stat_total_dict[goods_id]['repeat_order_num2'] = repeat_order_num2

            temp_res = self.col_NNWOrder.aggregate(
                [
                    {
                        "$match": {
                            "chan3_pin3_ID": goods_id,
                            "DelStatus": 0,
                            "PayStatus": 2,
                            "SalesVolume": {"$gt": 0},
                            'OrderTimeISODate': {
                                '$gte': self.OrderTimeISODate_begin_7,
                                '$lt': self.OrderTimeISODate_end_total,
                            },
                        }
                    },
                    {
                        "$group": {
                            "_id": "$you2_ke4_shou3_ji1_hao4",
                            "you2_ke4_shou3_ji1_hao4": {"$sum": 1},
                        },
                    },
                    {
                        "$group": {
                            "_id": None,
                            "total_customer": {"$sum": 1},
                        }
                    },
                ]
            )

            # 下单会员数量
            total_customer = 0
            for temp_one in temp_res:
                total_customer = temp_one['total_customer']
                break

            goods_stat_total_dict[goods_id]['total_customer'] = total_customer
            try:
                goods_stat_total_dict[goods_id]['RepeatOrderRate'] = round(repeat_order_num / total_customer, 4) * 100
            except:
                goods_stat_total_dict[goods_id]['RepeatOrderRate'] = 0

            try:
                goods_stat_total_dict[goods_id]['RepeatOrderRate2'] = round(
                    repeat_order_num2 / goods_stat_total_dict[goods_id]['order_num_total'], 4) * 100
            except:
                goods_stat_total_dict[goods_id]['RepeatOrderRate2'] = 0

            if goods_stat_total_dict[goods_id]['RepeatOrderRate'] == 0:
                goods_stat_total_dict[goods_id]['RepeatOrderRate'] = ''
            if goods_stat_total_dict[goods_id]['RepeatOrderRate2'] == 0:
                goods_stat_total_dict[goods_id]['RepeatOrderRate2'] = ''

        res = self.col_NNWOrder.aggregate(
            [
                {
                    "$match": {
                        "DelStatus": 0,
                        "PayStatus": 2,
                        "chan3_pin3_ID": {"$ne": "4056068"},
                        "SalesVolume": {"$gt": 0},
                        'OrderTimeISODate': {
                            '$gte': self.OrderTimeISODate_begin,
                            '$lt': self.OrderTimeISODate_end,
                        },
                    }
                },
                {
                    "$group": {
                        "_id": "$chan3_pin3_ID",
                        "order_num": {"$sum": 1},
                        "sell_num": {"$sum": "$SalesVolume"},
                        "BusinessTurnover": {"$sum": "$BusinessTurnover"},
                        "BusinessTurnoverCost": {"$sum": "$BusinessTurnoverCost"},
                        "GrossProfit": {"$sum": "$GrossProfit"},
                        "name": {"$first": "$ming2_cheng1"},
                        "chan3_pin3_ID": {"$first": "$chan3_pin3_ID"},
                        "skuname": {"$first": "$SKU_ming2_cheng1"}
                    }
                }
            ]
        )

        goods_dict = {}
        for one in res:
            goods_id = str(one['chan3_pin3_ID'])
            goods_dict[goods_id] = one

            # goods_dict[goods_id]['order_num_total'] = goods_stat_total_dict[goods_id]['order_num_total']
            # goods_dict[goods_id]['sell_num_total'] = goods_stat_total_dict[goods_id]['sell_num_total']
            # goods_dict[goods_id]['BusinessTurnover_total'] = goods_stat_total_dict[goods_id]['BusinessTurnover_total']
            # goods_dict[goods_id]['BusinessTurnoverCost_total'] = goods_stat_total_dict[goods_id]['BusinessTurnoverCost_total']
            # goods_dict[goods_id]['GrossProfit_total'] = goods_stat_total_dict[goods_id]['GrossProfit_total']

        for d in goods_stat_total_dict:
            if d in goods_dict:
                goods_stat_total_dict[d]['order_num'] = goods_dict[d]['order_num']
                goods_stat_total_dict[d]['sell_num'] = goods_dict[d]['sell_num']
                goods_stat_total_dict[d]['BusinessTurnover'] = goods_dict[d]['BusinessTurnover']
                goods_stat_total_dict[d]['BusinessTurnoverCost'] = goods_dict[d]['BusinessTurnoverCost']
                goods_stat_total_dict[d]['GrossProfit'] = goods_dict[d]['GrossProfit']
            else:
                goods_stat_total_dict[d]['order_num'] = 0
                goods_stat_total_dict[d]['sell_num'] = 0
                goods_stat_total_dict[d]['BusinessTurnover'] = 0
                goods_stat_total_dict[d]['BusinessTurnoverCost'] = 0
                goods_stat_total_dict[d]['GrossProfit'] = 0

        goods_list = []
        for d in goods_stat_total_dict:
            goods_list.append(goods_stat_total_dict[d])

        res = sorted(goods_list, key=operator.itemgetter('BusinessTurnover'), reverse=True)

        return res

    def do_export_excel_detail_total(self, data_list, worksheet):
        # 标题处理开始
        # 完整
        # column_name = ["订单ID", "码号", "二维码", "备注标签", "备注", "用户备注标签", "用户备注", "供应商备注标签", "供应商备注", "产品ID", "联票ID", "SKU名称",
        #                "发送费", "采购系统发送费", "单价", "总金额", "每份返现金额", "返现总金额", "采购价", "采购总额", "零售价格", "零售总金额", "利润", "总利润",
        #                "零售利润", "零售总利润", "总数", "可使用数", "已使用数", "申请退单数", "已退单数", "支付状态", "支付方式", "发送类型", "发送状态", "用户ID",
        #                "用户", "分组ID", "分组", "上级分组ID", "上级分组", "上级用户ID", "上级用户", "操作员ID", "操作员", "操作员分组", "供应商ID", "供应商",
        #                "供应商分组", "景区", "联系人", "联系人手机号", "联系人身份证号", "游客姓名", "游客手机号", "游客身份证号", "预计游玩时间", "开始日期", "过期日期",
        #                "每天开始时间", "每天停止时间", "有效星期", "座位号", "分销对接系统", "分销订单ID", "采购对接系统", "采购订单ID", "状态", "产品名称", "验证时间",
        #                "下单时间", "更新时间", "快递地址", "车牌号", "系统备注", "佣金比例", "联票佣金比例", "产品分类", "分享员账号", "分享员ID", "采购编码",
        #                "游玩时间段", "接送地址", "附加成本", "推广成本", "发票信息", "预约信息", "快递单号", "销量", "营业额", "营业额成本", "毛利", "下单人id",
        #                "归属人", "归属人id",
        #                "归属人分组",
        #                "订单数"]

        column_name = ["订单ID", "备注标签", "备注", "用户备注标签", "用户备注", "供应商备注标签", "供应商备注", "产品ID", "SKU名称",
                       "发送费", "采购系统发送费", "单价", "总金额", "每份返现金额", "返现总金额", "采购价", "采购总额", "零售价格", "零售总金额", "利润", "总利润",
                       "零售利润", "零售总利润", "总数", "可使用数", "已使用数", "申请退单数", "已退单数", "供应商ID", "供应商",
                       "供应商分组", "联系人", "联系人手机号", "联系人身份证号", "游客姓名", "游客手机号", "游客身份证号", "产品名称",
                       "下单时间", "更新时间", "快递地址", "系统备注", "佣金比例", "联票佣金比例", "产品分类", "分享员账号", "分享员ID", "采购编码",
                       "游玩时间段", "接送地址", "附加成本", "推广成本", "发票信息", "预约信息", "快递单号", "销量", "营业额", "营业额成本", "毛利", "下单人id",
                       "归属人", "归属人id",
                       "归属人分组",
                       "订单数"]

        i = 0
        for column in column_name:
            worksheet.write(0, i, column, self.style_column_bkg)
            i += 1
        # 标题处理结束

        # 数据处理开始
        line = 1

        # 完整
        # v_column = ["ID", "zhi1_fu4_liu2_shui3_hao4", "ma3_hao4", "er4_wei2_ma3", "bei4_zhu4_biao1_qian1", "bei4_zhu4",
        #             "yong4_hu4_bei4_zhu4_biao1_qian1", "yong4_hu4_bei4_zhu4",
        #             "gong1_ying4_shang1_bei4_zhu4_biao1_qian1", "gong1_ying4_shang1_bei4_zhu4", "chan3_pin3_ID",
        #             "SKU_ming2_cheng1", "fa1_song4_fei4", "cai3_gou4_xi4_tong3_fa1_song4_fei4",
        #             "dan1_jia4", "zong3_jin1_e2", "mei3_fen4_fan3_xian4_jin1_e2", "fan3_xian4_zong3_jin1_e2",
        #             "cai3_gou4_jia4", "cai3_gou4_zong3_e2", "ling2_shou4_jia4_ge2", "ling2_shou4_zong3_jin1_e2",
        #             "li4_run4", "zong3_li4_run4", "ling2_shou4_li4_run4", "ling2_shou4_zong3_li4_run4", "zong3_shu4",
        #             "ke3_shi3_yong4_shu4", "yi3_shi3_yong4_shu4", "shen1_qing3_tui4_dan1_shu4", "yi3_tui4_dan1_shu4",
        #             "zhi1_fu4_zhuang4_tai4", "zhi1_fu4_fang1_shi4", "fa1_song4_lei4_xing2", "fa1_song4_zhuang4_tai4",
        #             "yong4_hu4_ID", "yong4_hu4", "fen1_zu3_ID", "fen1_zu3", "shang4_ji2_fen1_zu3_ID",
        #             "shang4_ji2_fen1_zu3", "shang4_ji2_yong4_hu4_ID", "shang4_ji2_yong4_hu4", "cao1_zuo4_yuan2_ID",
        #             "cao1_zuo4_yuan2", "cao1_zuo4_yuan2_fen1_zu3", "gong1_ying4_shang1_ID", "gong1_ying4_shang1",
        #             "gong1_ying4_shang1_fen1_zu3", "jing3_qu1", "lian2_xi4_ren2", "lian2_xi4_ren2_shou3_ji1_hao4",
        #             "lian2_xi4_ren2_shen1_fen4_zheng4_hao4", "you2_ke4_xing4_ming2", "you2_ke4_shou3_ji1_hao4",
        #             "you2_ke4_shen1_fen4_zheng4_hao4", "yu4_ji4_you2_wan2_shi2_jian1", "kai1_shi3_ri4_qi1",
        #             "guo4_qi1_ri4_qi1", "mei3_tian1_kai1_shi3_shi2_jian1", "mei3_tian1_ting2_zhi3_shi2_jian1",
        #             "you3_xiao4_xing1_qi1", "zuo4_wei4_hao4", "fen1_xiao1_dui4_jie1_xi4_tong3",
        #             "fen1_xiao1_ding4_dan1_ID", "cai3_gou4_dui4_jie1_xi4_tong3", "cai3_gou4_ding4_dan1_ID",
        #             "zhuang4_tai4", "ming2_cheng1", "yan4_zheng4_shi2_jian1", "xia4_dan1_shi2_jian1",
        #             "geng4_xin1_shi2_jian1", "kuai4_di4_di4_zhi3", "che1_pai2_hao4", "xi4_tong3_bei4_zhu4",
        #             "yong4_jin1_bi3_li4", "lian2_piao4_yong4_jin1_bi3_li4", "chan3_pin3_fen1_lei4",
        #             "fen1_xiang3_yuan2_zhang4_hao4", "fen1_xiang3_yuan2_ID", "cai3_gou4_bian1_ma3",
        #             "you2_wan2_shi2_jian1_duan4", "jie1_song4_di4_zhi3", "fu4_jia1_cheng2_ben3",
        #             "tui1_guang3_cheng2_ben3", "fa1_piao4_xin4_xi1", "yu4_yue1_xin4_xi1", "kuai4_di4_dan1_hao4",
        #             "SalesVolume", "BusinessTurnover", "BusinessTurnoverCost", "GrossProfit", "OrderCustomerId",
        #             "BelongName", "BelongId", "BelongGroup",
        #             "OrderNum"]

        v_column = ["ID", "bei4_zhu4_biao1_qian1", "bei4_zhu4",
                    "yong4_hu4_bei4_zhu4_biao1_qian1", "yong4_hu4_bei4_zhu4",
                    "gong1_ying4_shang1_bei4_zhu4_biao1_qian1", "gong1_ying4_shang1_bei4_zhu4", "chan3_pin3_ID",
                    "SKU_ming2_cheng1", "fa1_song4_fei4", "cai3_gou4_xi4_tong3_fa1_song4_fei4",
                    "dan1_jia4", "zong3_jin1_e2", "mei3_fen4_fan3_xian4_jin1_e2", "fan3_xian4_zong3_jin1_e2",
                    "cai3_gou4_jia4", "cai3_gou4_zong3_e2", "ling2_shou4_jia4_ge2", "ling2_shou4_zong3_jin1_e2",
                    "li4_run4", "zong3_li4_run4", "ling2_shou4_li4_run4", "ling2_shou4_zong3_li4_run4", "zong3_shu4",
                    "ke3_shi3_yong4_shu4", "yi3_shi3_yong4_shu4", "shen1_qing3_tui4_dan1_shu4", "yi3_tui4_dan1_shu4",
                    "gong1_ying4_shang1_ID", "gong1_ying4_shang1",
                    "gong1_ying4_shang1_fen1_zu3", "lian2_xi4_ren2", "lian2_xi4_ren2_shou3_ji1_hao4",
                    "lian2_xi4_ren2_shen1_fen4_zheng4_hao4", "you2_ke4_xing4_ming2", "you2_ke4_shou3_ji1_hao4",
                    "you2_ke4_shen1_fen4_zheng4_hao4", "ming2_cheng1", "xia4_dan1_shi2_jian1",
                    "geng4_xin1_shi2_jian1", "kuai4_di4_di4_zhi3", "xi4_tong3_bei4_zhu4",
                    "yong4_jin1_bi3_li4", "lian2_piao4_yong4_jin1_bi3_li4", "chan3_pin3_fen1_lei4",
                    "fen1_xiang3_yuan2_zhang4_hao4", "fen1_xiang3_yuan2_ID", "cai3_gou4_bian1_ma3",
                    "you2_wan2_shi2_jian1_duan4", "jie1_song4_di4_zhi3", "fu4_jia1_cheng2_ben3",
                    "tui1_guang3_cheng2_ben3", "fa1_piao4_xin4_xi1", "yu4_yue1_xin4_xi1", "kuai4_di4_dan1_hao4",
                    "SalesVolume", "BusinessTurnover", "BusinessTurnoverCost", "GrossProfit", "OrderCustomerId",
                    "BelongName", "BelongId", "BelongGroup",
                    "OrderNum"]

        for i in range(len(data_list)):
            data = data_list[i]
            column = 0

            for j in range(len(v_column)):
                try:
                    worksheet.write(line, column, data[v_column[j]])
                except:
                    print(json_util.dumps(data, ensure_ascii=False))
                    exit()
                column += 1

            line += 1
        # 数据处理结束

        return worksheet

    def do_export_excel_detail_simple(self, data_list, worksheet):
        # 标题处理开始
        column_name = [{"name": "订单ID", "width": 10}, {"name": "下单时间", "width": 20}, {"name": "归属人", "width": 10},
                       {"name": "所属组", "width": 10}, {"name": "产品名称", "width": 30}, {"name": "详细名称", "width": 20},
                       {"name": "销售数", "width": 10}, {"name": "销售单价", "width": 12}, {"name": "采购单价", "width": 12},
                       {"name": "销售金额", "width": 12}, {"name": "收款金额", "width": 12}, {"name": "收款额成本", "width": 12},
                       {"name": "毛利", "width": 12}]

        i = 0
        for column in column_name:
            worksheet.write(0, i, column['name'], self.style_column_bkg)
            first_col = worksheet.col(i)
            first_col.width = 256 * column['width']
            i += 1
        # 标题处理结束

        # 数据处理开始
        line = 1

        v_column = ["ID", "xia4_dan1_shi2_jian1", "BelongName", "BelongGroup", "ming2_cheng1", "SKU_ming2_cheng1", 'SalesVolume',
                    "ling2_shou4_jia4_ge2", 'cai3_gou4_jia4', "ling2_shou4_zong3_jin1_e2", 'BusinessTurnover', 'BusinessTurnoverCost', 'GrossProfit']

        for i in range(len(data_list)):
            data = data_list[i]
            column = 0

            for j in range(len(v_column)):
                try:
                    if (j == 2 or 3) and data[v_column[j]] == "":
                        data[v_column[j]] = "无主销售"
                    worksheet.write(line, column, data[v_column[j]])
                except:
                    print(json_util.dumps(data, ensure_ascii=False))
                    exit()
                column += 1

            line += 1
        # 数据处理结束
        return worksheet

    def do_export_excel_goods(self, data_list, worksheet):
        # 标题处理开始
        column_name = [
            {"name": "排名", "width": 6}, {"name": "名称", "width": 20}, {"name": "订单数", "width": 10},
            {"name": "累计订单数", "width": 12}, {"name": "销量", "width": 10}, {"name": "累计销量", "width": 12},
            {"name": "采购额", "width": 12}, {"name": "累积采购额", "width": 15}, {"name": "毛利", "width": 12},
            {"name": "累计毛利", "width": 12}, {"name": "营业额", "width": 12}, {"name": "累计营业额", "width": 15},
            {"name": "复购率/%", "width": 12}, {"name": "订单复购率/%", "width": 12}, {"name": "复购用户数", "width": 12},
            {"name": "总用户数", "width": 12}, {"name": "重复订单数", "width": 12}
        ]

        i = 0
        for column in column_name:
            worksheet.write(0, i, column['name'], self.style_column_bkg)
            first_col = worksheet.col(i)
            first_col.width = 256 * column['width']
            i += 1
        # 标题处理结束

        # 数据处理开始
        line = 1
        for i in range(len(data_list)):
            data = data_list[i]
            column = 0

            worksheet.write(line, column, i + 1)
            column += 1

            v_column = ["name", "order_num", "order_num_total", "sell_num", "sell_num_total",
                        "BusinessTurnoverCost", "BusinessTurnoverCost_total", "GrossProfit", "GrossProfit_total",
                        "BusinessTurnover", "BusinessTurnover_total", "RepeatOrderRate", "RepeatOrderRate2",
                        "repeat_num","total_customer", "repeat_order_num2"]

            for j in range(len(v_column)):
                try:
                    worksheet.write(line, column, data[v_column[j]])
                except:
                    print(json_util.dumps(data, ensure_ascii=False))
                    exit()
                column += 1

            line += 1
        # 数据处理结束

        return worksheet

    def deal_sheet_goods(self, data_list):
        goods_object = {}
        for i in range(len(data_list)):
            data = data_list[i]
            goods_name = data['ming2_cheng1']

            if goods_name in goods_object:
                goods_object[goods_name]['Name'] = goods_name
                goods_object[goods_name]['OrderNum'] += 1
                goods_object[goods_name]['SaleNum'] += data['SalesVolume']
                goods_object[goods_name]['Cost'] += data['cai3_gou4_jia4'] * data['SalesVolume']
                goods_object[goods_name]['BusinessTurnover'] += data['BusinessTurnover']
            else:
                goods_object[goods_name] = {}
                goods_object[goods_name]['Name'] = goods_name
                goods_object[goods_name]['OrderNum'] = 1
                goods_object[goods_name]['SaleNum'] = data['SalesVolume']
                goods_object[goods_name]['Cost'] = data['cai3_gou4_jia4'] * data['SalesVolume']
                goods_object[goods_name]['BusinessTurnover'] = data['BusinessTurnover']

        goods_list = []
        for d in goods_object:
            goods_object[d]['GrossProfit'] = goods_object[d]['BusinessTurnover'] - goods_object[d]['Cost']
            goods_list.append(goods_object[d])

        # 排序
        res = sorted(goods_list, key=operator.itemgetter('BusinessTurnover'), reverse=True)

        return res

    def deal_sheet_group(self, res, group_class=1):
        temp_condition = {
            "DelStatus": 0,
            "GroupClass": group_class,
            'ShowStatus': 1,
        }

        temp_res = self.col_NNWGroup.find(temp_condition)
        temps = ["章丹丹组","谢爱娜组","胡丹珠组","陈诗瑜组","张蓓蓓组","张栋组","吴敏组","陈丽娜组","徐涯丹组","钱咪咪组","吴烨运组",]
        group_object = {}
        for one in temp_res:

            group_object[str(one["GroupName"])] = {
                'GroupName': one["GroupName"],
                'OrderNum': 0,
                'OrderNumTotal': 0,
                'SaleNum': 0,
                'SaleNumTotal': 0,
                'AccountToday': float(0),
                'AccountTotal': float(0),
            }

        for i in range(len(res)):
            data = res[i]

            if str(data['BelongGroup']) in group_object :
                temp_index = str(data['BelongGroup'])
                if group_class ==1:
                    group_object[temp_index]['AccountTotal'] += data['AccountTotal_newGroup']
                    group_object[temp_index]['OrderNumTotal'] += data['OrderNumTotal_newGroup']
                    group_object[temp_index]['SaleNumTotal'] += data['SaleNumTotal_newGroup']
                elif group_class == 2:
                    group_object[temp_index]['AccountTotal'] += data['AccountTotal']
                    group_object[temp_index]['OrderNumTotal'] += data['OrderNumTotal']
                    group_object[temp_index]['SaleNumTotal'] += data['SaleNumTotal']
                group_object[temp_index]['OrderNum'] += data['OrderNum']
                group_object[temp_index]['SaleNum'] += data['SaleNum']
                group_object[temp_index]['AccountToday'] += data['AccountToday']


        group_list = []
        for d in group_object:
            if group_class == 1 and d not in temps:
                continue
            group_list.append(group_object[d])

        # 排序
        res = sorted(group_list, key=operator.itemgetter('AccountToday'), reverse=True)

        return res

    def do_export_excel_group(self, group_list, worksheet):
        # 标题处理开始
        column_name = [
            {'name': '排名', 'width': 6},
            {'name': '分组', 'width': 10},
            {'name': '订单数', 'width': 10},
            {'name': '累积订单数', 'width': 13},
            {'name': '销量', 'width': 10},
            {'name': '累积销量', 'width': 10},
            {'name': '当日营业额', 'width': 13},
            {'name': '累积营业额', 'width': 13},
        ]

        i = 0
        for column in column_name:
            worksheet.write(0, i, column['name'], self.style_column_bkg)
            first_col = worksheet.col(i)
            first_col.width = 256 * column['width']
            i += 1
        # 标题处理结束

        # 数据处理开始
        line = 1
        for i in range(len(group_list)):
            data = group_list[i]
            column = 0

            worksheet.write(line, column, i + 1)
            column += 1

            for key in data:
                worksheet.write(line, column, data[key])
                column += 1

            line += 1
        # 数据处理结束

        return worksheet

    def do_export_excel_personnel(self, personnel_list, worksheet, upper_group_name=''):
        # 标题处理开始
        column_name = [
            {'name': '排名', 'width': 6},
            {'name': '归属人', 'width': 10},
            {'name': '所属分组', 'width': 10},
            {'name': '原分组', 'width': 10},
            {'name': '订单数', 'width': 10},
            {'name': '累积订单数', 'width': 13},
            {'name': '销量', 'width': 10},
            {'name': '累积销量', 'width': 13},
            {'name': '当日营业额', 'width': 13},
            {'name': '累积营业额', 'width': 13},
            {'name': '累积营业额(含退款)', 'width': 18},
            {'name': '南泥湾项目', 'width': 13},
            {'name': '职级', 'width': 10},
            {'name': '黄牌次数', 'width': 10},
        ]

        i = 0
        for column in column_name:
            worksheet.write(0, i, column['name'], self.style_column_bkg)
            first_col = worksheet.col(i)
            first_col.width = 256 * column['width']
            i += 1
        # 标题处理结束
        # 数据处理开始
        line = 1
        for i in range(len(personnel_list)):
            data = personnel_list[i]
            #个人统计不显示轮休的人员
            if upper_group_name == '' and data['HidePersonRank'] == 3:
                continue
            if data['HidePersonRank'] == 1:
                continue
            if data['HidePersonRank'] == 3:
                data['UpperGroup'] = "轮休组"

            if upper_group_name and upper_group_name != data['UpperGroup']:
                continue

            column = 0

            worksheet.write(line, column, line)
            column += 1

            temp_column_name = [
                "RealName",
                "BelongGroup",
                "Group",
                "OrderNum",
                "OrderNumTotal",
                "SaleNum",
                "SaleNumTotal",
                "AccountToday",
                "AccountTotal",
                "AccountIncludeRefunded",
                "BelongNNW",
                "JobGrade",
                "YellowCardTimesCurrent",
            ]
            for tcn in range(len(temp_column_name)):
                tcnv = temp_column_name[tcn]
                worksheet.write(line, column, data[tcnv])
                column += 1

            line += 1
        # 数据处理结束

        return worksheet

    def find_superior(self, id):
        if id in self.superior:
            if not self.superior[str(id)]:
                return False
            if str(self.superior[str(id)]) in self.old_object:
                return str(self.superior[str(id)])

            return self.find_superior(str(self.superior[str(id)]))

        return False

    def set_insert_data(self, data):
        # 处理没有收获地址

        param = {}
        param['OrderId'] = str(int(float(data[0])))
        param['ProductId'] = str(int(float(data[1])))
        param['SKUName'] = str(data[2])
        param['ProductName'] = str(data[3])
        param['ProductTitle'] = str(data[4])
        param['PlayTime'] = self.get_time(data[5], 1)
        param['LeaveTime'] = self.get_time(data[6], 1)
        param['NumberOfRooms'] = str(int(float(data[7])))
        param['TotalMoney'] = str(data[8])
        param['CustomerPay'] = str(data[9])
        param['TechnicalService'] = str(data[10])
        param['ActualToAccount'] = str(data[11])
        param['Subordinate'] = str(data[12])
        param['SubordinateCommission'] = str(data[13])
        param['ShareCustomerId'] = str(int(float(data[14]))) if data[14] else ''
        # param['ShareCustomerId'] = str(int(float(str(data[14]))))
        param['ShareCustomerName'] = str(data[15])
        param['ShareCustomerGroup'] = str(data[16])
        param['CustomerGroup'] = str(data[17])
        param['CustomerCommission'] = str(data[18])
        param['PromoteCustomerId'] = str(int(float(data[19]))) if data[19] else ''
        param['PromoteCustomerName'] = str(data[20])
        param['PromoteCustomerCommission'] = str(data[21])
        try:
            param['AwardCustomerId'] = str(int(float(data[22])))
        except:
            param['AwardCustomerId'] = '0'
        param['AwardMoney'] = str(data[23])
        param['AmountAfterDeductCommission'] = str(data[24])
        param['Price'] = str(data[25])
        param['TotalPrice'] = str(data[26])
        param['CustomerId'] = str(int(float(data[27])))
        param['CustomerName'] = str(data[28])
        param['CustomerContact'] = str(data[29])
        param['CustomerMobile'] = str(int(float(data[30])))
        param['TimesToUse'] = str(int(float(data[31])))
        param['TimesAlreadyUse'] = str(int(float(data[32])))
        param['ApplicationForRefundNumber'] = str(int(float(data[33])))
        param['AlreadyRefundNumber'] = str(int(float(data[34])))
        param['RefundTime'] = self.get_time(data[35], 1)
        param['OrderTime'] = self.get_time(data[36], 1)
        param['OrderTimeISODate'] = self.get_time(data[36], 2)
        param['OrderCustomerId'] = str(int(float(data[37])))
        param['DelStatus'] = 0

        return param

    def get_time(self, time, type=1):
        if not time:
            res = ''
        elif int(type) == 1:
            res = str(xldate_as_datetime(time, 0))
        else:
            res = xldate_as_datetime(time, 0).fromisoformat(str(xldate_as_datetime(time, 0)))

        return res

    def read_excel(self):
        wb = xlrd.open_workbook('F:\\project\\python_work_erp\\Temp\\order.xls')  # 打开Excel文件
        sheet = wb.sheet_by_name('order')  # 通过excel表格名称(rank)获取工作表
        dat = []  # 创建空list

        for a in range(sheet.nrows):  # 循环读取表格内容（每次读取一行数据）
            cells = sheet.row_values(a)  # 每行数据赋值给cells

            temp_list = []
            for temp_one in cells:
                data = temp_one  # 因为表内可能存在多列数据，0代表第一列数据，1代表第二列，以此类推
                temp_list.append(data)
            dat.append(temp_list)  # 把每次循环读取的数据插入到list

        return dat


if __name__ == '__main__':
    start = time.perf_counter()
    OrderStat = OrderStat()

    try:
        OrderStat.main()
    except BaseException as err:
        traceback.print_exc()
        print('__main__ exception: ')
        print(err)
    finally:
        elapsed = (time.perf_counter() - start)
        print("Python Complete, Time used:", elapsed)
