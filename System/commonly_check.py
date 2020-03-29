# coding: utf-8
"""
进行一些常用检查
1. check_call_service 检查未接来电服务是否可能挂掉
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import requests
import random
from pika.compat import xrange
from pymongo import UpdateOne, CursorType
from common import handle_mongodb
from common.readconfig import ReadConfig
from bson import json_util
from bson.objectid import ObjectId
import time
import datetime
import json
from pytz import timezone
from common import send_email

cst_tz = timezone('Asia/Shanghai')


class CommonlyCheck:
    def __init__(self):
        self.system_id = '000000000000000000002251'
        self.IM_URL = str(ReadConfig().get_url("im_url"))
        self.mongodb = handle_mongodb.HandleMongoDB()
        self.mongodb.mongodb_connect()

        self.col_CallRecord = self.mongodb.select_col('CallRecord')

        self.now_timestamp = int(time.time())
        self.today_start_timestamp = int(time.mktime(time.strptime(str(datetime.date.today()), '%Y-%m-%d'))) - 1 + 1

        year = datetime.datetime.now().year
        month = datetime.datetime.now().month
        day = datetime.datetime.now().day
        self.hour = datetime.datetime.now().hour
        minute = datetime.datetime.now().minute
        second = datetime.datetime.now().second
        self.now_iso_date = cst_tz.localize(datetime.datetime(year, month, day, self.hour, minute, second))

        self.final_message = []

    def main(self):
        if self.hour >= 10 and self.hour <= 18:
            CC.check_call_service()

        CC.send_email()

    def check_call_service(self):
        print((datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S') + ' check_call_service start')

        condition = {
            "CallType.CallTypeCode": 0,
            "$and": [
                {
                    "CallerID": {
                        "$ne": "02388694959"
                    }
                }
            ],
            "CallingSystemAPIData.IO": 0,
            "CallStatus.CallStatusCode": 0
        }

        projection = {
            'CallTime': 1,
        }

        sort = [('_id', -1)]

        res = self.col_CallRecord.find_one(condition, projection, sort=sort)

        last_call_time = res['CallTime']

        last_call_time = str(last_call_time)
        if '.' in last_call_time:
            last_call_time = last_call_time.split('.')[0]
            
        last_call_time = time.mktime(time.strptime(last_call_time, "%Y-%m-%d %H:%M:%S")) + 8 * 3600
        
        if self.now_timestamp > (last_call_time + 8 * 3600):
            msg = 'check_call_service: 未接来电近8小时没有数据, 服务可能异常'
            print(msg)
            self.final_message.append(msg)

        print((datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S') + ' check_call_service complete')

    def send_email(self):
        print('final_message:')
        print(json_util.dumps(self.final_message, ensure_ascii=False))

        if self.final_message:
            content = ''

            for msg in self.final_message:
                content = content + msg + "\r\n"

            text = {
                'title': 'Python CommonlyCheck: ' + (datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S'),
                'content': content,
            }
            send_email.EmailSmtp().send('854641898@qq.com', text)

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

            print('发送通知: ')
            print(message)
            print(res.text)
        except BaseException as err:
            print('main send_message: ')
            print(err)


if __name__ == '__main__':
    print((datetime.datetime.now()).strftime('%Y-%m-%d %H:%M:%S') + ' Python Server Start')
    start = time.perf_counter()
    CC = CommonlyCheck()

    try:
        CC.main()
    except BaseException as err:
        print('__main__ exception: ')
        print(err)
    finally:
        elapsed = (time.perf_counter() - start)
        print("Python Server Complete Time used:", elapsed)
