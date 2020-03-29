# coding: utf-8
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

class Send:
    def __init__(self):
        self.system_id = '000000000000000000002251'
        self.IM_URL = str(ReadConfig().get_url("im_url"))
        self.mongodb = handle_mongodb.HandleMongoDB()
        self.mongodb.mongodb_connect()

        self.col_Personnel = self.mongodb.select_col('Personnel')
        self.col_Department = self.mongodb.select_col('Department')
        self.col_KPIPersonnelXref = self.mongodb.select_col('KPIPersonnelXref')
        self.col_KPIScoreUserXref = self.mongodb.select_col('KPIScoreUserXref')

        self.year = datetime.datetime.now().year
        self.month = datetime.datetime.now().month
        if self.month == 1:
            self.year = self.year - 1
            self.month = 12
        else:
            self.month = self.month - 1

        # self.year = 2019
        # self.month = 12

        try:
            if int(sys.argv[1]) > 0:
                self.month = int(sys.argv[1])
        except Exception as e:
            pass

        try:
            if int(sys.argv[2]) > 0:
                self.year = int(sys.argv[2])
        except Exception as e:
            pass

    def main(self):
        temp_condition = {
            'DelStatus': 0,
            'IsSummary': 0,
            'KPIScoreTypeID': {
                '$in': [30, 40, 50]
            },
            "CheckMonth": self.month,
            "CheckYear": self.year,
        }

        lookup = {
            'from': "KPIPersonnelXref",
            "localField": "KPIPersonnelID",
            "foreignField": "_id",
            "as": "TargetPersonnelID",
        }

        temp_project = {
            'ScoreUser': 1,
            'TargetPersonnelID.KPIUser.ForeignKeyID': 1,
            'TargetPersonnelID.KPIUser.EmployeeName': 1,
        }

        unwind = {
            '$unwind': '$TargetPersonnelID',
        }

        list = self.col_KPIScoreUserXref.aggregate(
            [
                {'$match': temp_condition},
                {'$lookup': lookup},
                {'$project': temp_project},
                unwind,
            ]
        )

        messages = []
        for one in list:
            recver = ''
            target_name = ''
            targetId = ''

            try:
                targetId = str(one['TargetPersonnelID']['KPIUser']['ForeignKeyID'])
                target_name = str(one['TargetPersonnelID']['KPIUser']['EmployeeName'])
                recver = str(one['ScoreUser']['ForeignKeyID'])
            except BaseException as err:
                print('main exception: ' + err)

            if not recver or not target_name or not targetId:
                continue

            title = str(self.year) + '年' + str(self.month) + '月份目标评分：请对' + target_name + '进行打分'

            temp_msg = {
                'sender': str(self.system_id),
                'recver': recver,
                'title': title,
                'content': title,
                'targetId': targetId,
                'messageType': 1,
                'type': 16,
                'subType': 116005,
                'hasBusinessId': False,
                'data': {
                    "CheckMonth": self.month,
                    "CheckYear": self.year,
                },
            }

            messages.append(temp_msg)

            if len(temp_msg) > 0:
                Send.send_message(messages)
                messages = []

    def send_message(self, message):
        """
        @name       发送通知
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

            print('发送给客户打标签通知: ')
            print(message)
            print(res.text)
        except BaseException as err:
            print('main send_message: ' + err)


if __name__ == '__main__':
    try:
        start = time.perf_counter()

        Send = Send()
        Send.main()
    except BaseException as err:
        print('__main__ exception: ')
        print(err)
    finally:
        elapsed = (time.perf_counter() - start)
        print("Python Complete, Time used:", elapsed)
