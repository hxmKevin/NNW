# coding:utf-8
# coding: utf-8
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
from pymongo import UpdateOne
from common import handle_mongodb
from common.readconfig import ReadConfig
import traceback
import random
from bson import json_util
from bson.objectid import ObjectId
import time
import datetime
import json
from xlrd import xldate_as_tuple, xldate_as_datetime
from pytz import timezone
from xpinyin import Pinyin
import requests
import bs4
import time
from common import send_email

cst_tz = timezone('Asia/Shanghai')


class ImportMain:
    def __init__(self):
        self.mongodb = handle_mongodb.HandleMongoDB()
        self.mongodb.mongodb_connect()

        self.file_path = str(ReadConfig().get_nnw_param("file_path"))
        self.zj_user_name = str(ReadConfig().get_nnw_param("zj_user_name"))
        self.zj_user_pwd = str(ReadConfig().get_nnw_param("zj_user_pwd"))

        self.now_iso_date = cst_tz.localize(datetime.datetime.now())
        now_time = datetime.datetime.now()

        self.year = datetime.datetime.now().year
        self.month = datetime.datetime.now().month
        self.day = datetime.datetime.now().day
        self.hour = datetime.datetime.now().hour
        self.minute = datetime.datetime.now().minute
        self.second = datetime.datetime.now().second

        self.datetime_before = now_time + datetime.timedelta(minutes=-40)
        self.datetime_before_1_day = now_time + datetime.timedelta(days=-1)
        self.datetime_before_2_day = now_time + datetime.timedelta(days=-2)
        self.datetime_before_3_day = now_time + datetime.timedelta(days=-3)
        # datetime_before = cst_tz.localize(datetime.datetime(2020, 2, 17, 17, 0, 0))

        self.year_before = self.datetime_before.year
        self.month_before = self.datetime_before.month
        self.day_before = self.datetime_before.day
        self.hour_before = self.datetime_before.hour
        self.minute_before = self.datetime_before.minute
        self.second_before = self.datetime_before.second

        self.col_NNWOrder = self.mongodb.select_col('NNWOrder')
        self.col_NNWIdName = self.mongodb.select_col('NNWIdName')
        self.col_CustomSettings = self.mongodb.select_col('CustomSettings')
        self.col_NNWCustomer = self.mongodb.select_col('NNWCustomer')
        self.col_NNWGroup = self.mongodb.select_col('NNWGroup')

        self.field_list = ["ID","ma3_hao4","er4_wei2_ma3","bei4_zhu4_biao1_qian1","bei4_zhu4","yong4_hu4_bei4_zhu4_biao1_qian1","yong4_hu4_bei4_zhu4","gong1_ying4_shang1_bei4_zhu4_biao1_qian1","gong1_ying4_shang1_bei4_zhu4","chan3_pin3_ID","lian2_piao4_ID","SKU_ming2_cheng1","fa1_song4_fei4","cai3_gou4_xi4_tong3_fa1_song4_fei4","dan1_jia4","zong3_jin1_e2","mei3_fen4_fan3_xian4_jin1_e2","fan3_xian4_zong3_jin1_e2","cai3_gou4_jia4","cai3_gou4_zong3_e2","ling2_shou4_jia4_ge2","ling2_shou4_zong3_jin1_e2","li4_run4","zong3_li4_run4","ling2_shou4_li4_run4","ling2_shou4_zong3_li4_run4","zong3_shu4","ke3_shi3_yong4_shu4","yi3_shi3_yong4_shu4","shen1_qing3_tui4_dan1_shu4","yi3_tui4_dan1_shu4","zhi1_fu4_zhuang4_tai4","zhi1_fu4_fang1_shi4","fa1_song4_lei4_xing2","fa1_song4_zhuang4_tai4","yong4_hu4_ID","yong4_hu4","fen1_zu3_ID","fen1_zu3","shang4_ji2_fen1_zu3_ID","shang4_ji2_fen1_zu3","shang4_ji2_yong4_hu4_ID","shang4_ji2_yong4_hu4","cao1_zuo4_yuan2_ID","cao1_zuo4_yuan2","cao1_zuo4_yuan2_fen1_zu3","gong1_ying4_shang1_ID","gong1_ying4_shang1","gong1_ying4_shang1_fen1_zu3","jing3_qu1","lian2_xi4_ren2","lian2_xi4_ren2_shou3_ji1_hao4","lian2_xi4_ren2_shen1_fen4_zheng4_hao4","you2_ke4_xing4_ming2","you2_ke4_shou3_ji1_hao4","you2_ke4_shen1_fen4_zheng4_hao4","yu4_ji4_you2_wan2_shi2_jian1","kai1_shi3_ri4_qi1","guo4_qi1_ri4_qi1","mei3_tian1_kai1_shi3_shi2_jian1","mei3_tian1_ting2_zhi3_shi2_jian1","you3_xiao4_xing1_qi1","zuo4_wei4_hao4","fen1_xiao1_dui4_jie1_xi4_tong3","fen1_xiao1_ding4_dan1_ID","cai3_gou4_dui4_jie1_xi4_tong3","cai3_gou4_ding4_dan1_ID","zhuang4_tai4","ming2_cheng1","yan4_zheng4_shi2_jian1","xia4_dan1_shi2_jian1","geng4_xin1_shi2_jian1","kuai4_di4_di4_zhi3","che1_pai2_hao4","xi4_tong3_bei4_zhu4","yong4_jin1_bi3_li4","lian2_piao4_yong4_jin1_bi3_li4","chan3_pin3_fen1_lei4","fen1_xiang3_yuan2_zhang4_hao4","fen1_xiang3_yuan2_ID","cai3_gou4_bian1_ma3","you2_wan2_shi2_jian1_duan4","jie1_song4_di4_zhi3","fu4_jia1_cheng2_ben3","tui1_guang3_cheng2_ben3","fa1_piao4_xin4_xi1","yu4_yue1_xin4_xi1","kuai4_di4_dan1_hao4","zhi1_fu4_liu2_shui3_hao4","hui4_yuan2_xin4_xi1"]
        self.field_list_float = ["fa1_song4_fei4","cai3_gou4_xi4_tong3_fa1_song4_fei4","dan1_jia4","zong3_jin1_e2","mei3_fen4_fan3_xian4_jin1_e2","fan3_xian4_zong3_jin1_e2","cai3_gou4_jia4","cai3_gou4_zong3_e2","ling2_shou4_jia4_ge2","ling2_shou4_zong3_jin1_e2","li4_run4","zong3_li4_run4","ling2_shou4_li4_run4","ling2_shou4_zong3_li4_run4"]
        self.field_list_int = ["zong3_shu4","ke3_shi3_yong4_shu4","yi3_shi3_yong4_shu4","shen1_qing3_tui4_dan1_shu4","yi3_tui4_dan1_shu4"]

