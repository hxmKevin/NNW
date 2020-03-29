# coding: utf-8
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
import random
from pika.compat import xrange
from pymongo import UpdateOne
from common import handle_mongodb
from bson import json_util
from bson.objectid import ObjectId
import time
import datetime


class Kpi:
    def __init__(self):
        self.mongodb = handle_mongodb.HandleMongoDB()
        self.mongodb.mongodb_connect()

        self.col_Personnel = self.mongodb.select_col('Personnel')
        self.col_Department = self.mongodb.select_col('Department')
        self.col_KPIPersonnelXref = self.mongodb.select_col('KPIPersonnelXref')
        self.col_KPIScoreUserXref = self.mongodb.select_col('KPIScoreUserXref')
        # self.col_KPIPersonnelXref = self.mongodb.select_col('KPIPersonnelXrefCopy')
        # self.col_KPIScoreUserXref = self.mongodb.select_col('KPIScoreUserXrefCopy')

        self.sync_no = int(round(time.time() * 1000))

        self.year = datetime.datetime.now().year
        self.month = datetime.datetime.now().month
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

        self.KPIPersonnelXref_common_op_data = {
            'CheckYear': self.year,
            'CheckMonth': self.month,
        }

        self.pre_department_list = Kpi.get_pre_department_list(self)

    def get_pre_department_list(self):
        temp_condition = {
            'IsDel': 0,
        }

        temp_project = {
            'Name': 1,
        }

        res = self.col_Department.find(temp_condition, temp_project)

        pre_department_list = {}
        for one in res:
            pre_department_list[str(one['_id'])] = one['Name']

        return pre_department_list

    def get_personnel_list(self):
        condition = {
            'Lock': 0,
            'DelStatus': 0,
            'ParticipateInKPI': True,
            # 'GradeLeader.ForeignKeyID': {'$exists': True},
            # 'GradeLeader.EmployeeName': {'$exists': True},
        }

        lookup = {
            'from': "Department",
            "localField": "Department.ID",
            "foreignField": "_id",
            "as": "DepartmentData",
        }

        project = {
            'FullName': 1,
            'Department': 1,
            'DepartmentData.Name': 1,
            'GradeLeader.ForeignKeyID': 1,
            'GradeLeader.EmployeeName': 1,
            'GradeLeader.EmployeeDepartmentID': 1,
        }

        unwind = {
            '$unwind': '$DepartmentData',
        }

        list = self.col_Personnel.aggregate(
            [
                {'$match': condition},
                {'$lookup': lookup},
                {'$project': project},
                unwind,
            ]
        )

        return list

    def handle_KPIPersonnelXref(self, personnel_list):
        """
        @name   将参与考核的人存入KPIPersonnelXref表
        """
        personnel_bulk = []
        length = len(personnel_list)
        for i in xrange(length):
            this_personnel = {
                'ForeignKeyID': ObjectId(personnel_list[i]['_id']),
                'EmployeeName': str(personnel_list[i]['FullName']),
                'EmployeeDepartmentID': ObjectId(personnel_list[i]['Department']['ID']),
                'EmployeeDepartmentName': str(personnel_list[i]['DepartmentData']['Name']),
            }

            temp_one = UpdateOne(
                {
                    'KPIUser.ForeignKeyID': ObjectId(personnel_list[i]['_id']),
                    'CheckYear': int(self.year),
                    'CheckMonth': int(self.month),
                },
                {
                    '$set': {
                        'DelStatus': 0,
                        'KPIUser': this_personnel,
                        'BatchNO': self.sync_no,
                    }
                },
                True,
            )
            personnel_bulk.append(temp_one)

        if len(personnel_bulk) > 0:
            self.col_KPIPersonnelXref.bulk_write(personnel_bulk)

    def get_kpi_related_list(self):
        """
        @name   KPIPersonnelXref 表中 _id 与 员工_id 的对应
        """
        temp_condition = {
            'CheckYear': self.year,
            'CheckMonth': self.month,
            'DelStatus': 0,
        }

        res = self.col_KPIPersonnelXref.find(temp_condition)

        return_list = {}
        for one in res:
            index = str(one['KPIUser']['ForeignKeyID'])
            return_list[index] = str(one['_id'])

        return return_list

    def main(self):
        res = Kpi.get_personnel_list()
        personnel_list = []
        personnel_id_list = []
        for one in res:
            personnel_list.append(one)
            personnel_id_list.append(str(one['_id']))

        user_bulk = []

        ''' 评分领导人 和 评分下属 从属关系object   键 为 评分领导人(不一定设置为参与考核), 值为评分下属list(参与考核) '''
        pre_personnel_object = {}
        length = len(personnel_list)

        for i in xrange(length):
            if 'GradeLeader' in personnel_list[i]:
                index = str(personnel_list[i]['GradeLeader']['ForeignKeyID'])
                this_personnel = {
                    'ForeignKeyID': ObjectId(personnel_list[i]['_id']),
                    'EmployeeName': str(personnel_list[i]['FullName']),
                    'EmployeeDepartmentID': ObjectId(personnel_list[i]['Department']['ID']),
                    'EmployeeDepartmentName': str(personnel_list[i]['DepartmentData']['Name']),
                }

                if index in pre_personnel_object:
                    pre_personnel_object[index].append(this_personnel)
                else:
                    pre_personnel_object[index] = [
                        this_personnel
                    ]

        ''' 将参与考核的人存入KPIPersonnelXref表 '''
        Kpi.handle_KPIPersonnelXref(personnel_list)

        ''' 软删除 KPIPersonnelXref 表 无关数据 '''
        Kpi.del_unrelated_data_of_KPIPersonnelXref()

        kpi_related_list = Kpi.get_kpi_related_list()

        length = len(personnel_list)
        for i in xrange(length):
            this_personnel = {
                'ForeignKeyID': ObjectId(personnel_list[i]['_id']),
                'EmployeeName': str(personnel_list[i]['FullName']),
                'EmployeeDepartmentID': ObjectId(personnel_list[i]['Department']['ID']),
                'EmployeeDepartmentName': str(personnel_list[i]['DepartmentData']['Name']),
            }

            ''' 自我评价 '''
            temp_one = UpdateOne(
                {
                    'KPIPersonnelID': ObjectId(kpi_related_list[str(personnel_list[i]['_id'])]),
                    'KPIScoreTypeID': 10,
                    'ScoreUser.ForeignKeyID': this_personnel['ForeignKeyID'],
                    'IsSummary': 0,
                    'CheckYear': self.year,
                    'CheckMonth': self.month,
                },
                {
                    '$set': {
                        'DelStatus': 0,
                        'ScoreUser': this_personnel,
                        'BatchNO': self.sync_no,
                    }
                },
                True,
            )
            user_bulk.append(temp_one)

            ''' 自我评价汇总 '''
            temp_one = UpdateOne(
                {
                    'KPIPersonnelID': ObjectId(kpi_related_list[str(personnel_list[i]['_id'])]),
                    'KPIScoreTypeID': 10,
                    'IsSummary': 1,
                    'CheckYear': self.year,
                    'CheckMonth': self.month,
                },
                {
                    '$set': {
                        'DelStatus': 0,
                        'BatchNO': self.sync_no,
                    }
                },
                True,
            )
            user_bulk.append(temp_one)

            ''' 同事评价 '''
            temp_one = UpdateOne(
                {
                    'KPIPersonnelID': ObjectId(kpi_related_list[str(personnel_list[i]['_id'])]),
                    'KPIScoreTypeID': 20,
                    'IsSummary': 1,
                    'CheckYear': self.year,
                    'CheckMonth': self.month,
                },
                {
                    '$set': {
                        'DelStatus': 0,
                        'BatchNO': self.sync_no,
                    }
                },
                True,
            )
            user_bulk.append(temp_one)

            ''' 上级评价 '''
            if 'GradeLeader' in personnel_list[i]:
                # if str(personnel_list[i]['GradeLeader']['ForeignKeyID']) in personnel_id_list:
                personnel_list[i]['GradeLeader']['EmployeeDepartmentName'] = self.pre_department_list[str(personnel_list[i]['GradeLeader']['EmployeeDepartmentID'])]

                temp_one = UpdateOne(
                    {
                        'KPIPersonnelID': ObjectId(kpi_related_list[str(personnel_list[i]['_id'])]),
                        'KPIScoreTypeID': 30,
                        'ScoreUser.ForeignKeyID': personnel_list[i]['GradeLeader']['ForeignKeyID'],
                        'IsSummary': 0,
                        'CheckYear': self.year,
                        'CheckMonth': self.month,
                    },
                    {
                        '$set': {
                            'DelStatus': 0,
                            'ScoreUser': personnel_list[i]['GradeLeader'],
                            'BatchNO': self.sync_no,
                        }
                    },
                    True,
                )
                user_bulk.append(temp_one)

                ''' 上级评价汇总 '''
                temp_one = UpdateOne(
                    {
                        'KPIPersonnelID': ObjectId(kpi_related_list[str(personnel_list[i]['_id'])]),
                        'KPIScoreTypeID': 30,
                        'IsSummary': 1,
                        'CheckYear': self.year,
                        'CheckMonth': self.month,
                    },
                    {
                        '$set': {
                            'DelStatus': 0,
                            'BatchNO': self.sync_no,
                        }
                    },
                    True,
                )
                user_bulk.append(temp_one)

            ''' 同级评价 '''
            if 'GradeLeader' in personnel_list[i]:
                if str(personnel_list[i]['GradeLeader']['ForeignKeyID']) in pre_personnel_object:
                    peer_list = pre_personnel_object[str(personnel_list[i]['GradeLeader']['ForeignKeyID'])]
                    random.shuffle(peer_list)

                    limit = 0
                    for peer in peer_list:
                        if str(personnel_list[i]['_id']) == str(peer['ForeignKeyID']):
                            continue

                        limit += 1

                        temp_one = UpdateOne(
                            {
                                'KPIPersonnelID': ObjectId(kpi_related_list[str(personnel_list[i]['_id'])]),
                                'KPIScoreTypeID': 40,
                                'ScoreUser.ForeignKeyID': ObjectId(peer['ForeignKeyID']),
                                'IsSummary': 0,
                                'CheckYear': self.year,
                                'CheckMonth': self.month,
                            },
                            {
                                '$set': {
                                    'DelStatus': 0,
                                    'ScoreUser': peer,
                                    'BatchNO': self.sync_no,
                                }
                            },
                            True,
                        )
                        user_bulk.append(temp_one)
                        if limit >= 5:
                            break

                    ''' 同级评价汇总 '''
                    if limit > 0:
                        temp_one = UpdateOne(
                            {
                                'KPIPersonnelID': ObjectId(kpi_related_list[str(personnel_list[i]['_id'])]),
                                'KPIScoreTypeID': 40,
                                'IsSummary': 1,
                                'CheckYear': self.year,
                                'CheckMonth': self.month,
                            },
                            {
                                '$set': {
                                    'DelStatus': 0,
                                    'BatchNO': self.sync_no,
                                }
                            },
                            True,
                        )
                        user_bulk.append(temp_one)

            ''' 下级评价 '''
            if str(personnel_list[i]['_id']) in pre_personnel_object:
                subordinate_list = pre_personnel_object[str(personnel_list[i]['_id'])]
                random.shuffle(subordinate_list)

                limit = 0
                for subordinate in subordinate_list:
                    if str(personnel_list[i]['_id']) == str(subordinate['ForeignKeyID']):
                        continue

                    limit += 1

                    temp_one = UpdateOne(
                        {
                            'KPIPersonnelID': ObjectId(kpi_related_list[str(personnel_list[i]['_id'])]),
                            'KPIScoreTypeID': 50,
                            'ScoreUser.ForeignKeyID': ObjectId(subordinate['ForeignKeyID']),
                            'IsSummary': 0,
                            'CheckYear': self.year,
                            'CheckMonth': self.month,
                        },
                        {
                            '$set': {
                                'DelStatus': 0,
                                'ScoreUser': subordinate,
                                'BatchNO': self.sync_no,
                            }
                        },
                        True,
                    )
                    user_bulk.append(temp_one)
                    if limit >= 5:
                        break

                ''' 下级评价汇总 '''
                if limit > 0:
                    temp_one = UpdateOne(
                        {
                            'KPIPersonnelID': ObjectId(kpi_related_list[str(personnel_list[i]['_id'])]),
                            'KPIScoreTypeID': 50,
                            'IsSummary': 1,
                            'CheckYear': self.year,
                            'CheckMonth': self.month,
                        },
                        {
                            '$set': {
                                'DelStatus': 0,
                                'BatchNO': self.sync_no,
                            }
                        },
                        True,
                    )
                    user_bulk.append(temp_one)

            ''' 上下级评价 '''
            temp_one = UpdateOne(
                {
                    'KPIPersonnelID': ObjectId(kpi_related_list[str(personnel_list[i]['_id'])]),
                    'KPIScoreTypeID': 100,
                    'IsSummary': 1,
                    'CheckYear': self.year,
                    'CheckMonth': self.month,
                },
                {
                    '$set': {
                        'DelStatus': 0,
                        'BatchNO': self.sync_no,
                    }
                },
                True,
            )
            user_bulk.append(temp_one)

        if len(user_bulk) > 0:
            self.col_KPIScoreUserXref.bulk_write(user_bulk)

        ''' 软删除 KPIScoreUserXref 表的无关数据 '''
        Kpi.del_unrelated_data_of_KPIScoreUserXref()

    def del_unrelated_data_of_KPIPersonnelXref(self):
        """
        @name   软删除 KPIPersonnelXref 表 无关数据
        """
        temp_condition = {
            'DelStatus': 0,
            'CheckYear': self.year,
            'CheckMonth': self.month,
            'BatchNO': {'$ne': self.sync_no},
        }

        self.col_KPIPersonnelXref.update_many(
            temp_condition,
            {
                "$set": {
                    "DelStatus": 1,
                }
            },
        )

    def del_unrelated_data_of_KPIScoreUserXref(self):
        """
        @name   软删除 KPIScoreUserXref 表的无关数据
        """
        temp_condition = {
            'DelStatus': 0,
            'CheckYear': self.year,
            'CheckMonth': self.month,
            'BatchNO': {'$ne': self.sync_no},
        }

        self.col_KPIScoreUserXref.update_many(
            temp_condition,
            {
                "$set": {
                    "DelStatus": 1,
                }
            },
        )


if __name__ == '__main__':
    try:
        start = time.perf_counter()

        Kpi = Kpi()
        Kpi.main()
    except BaseException as err:
        print('__main__ exception: ')
        print(err)
    finally:
        elapsed = (time.perf_counter() - start)
        print("Python Complete, Time used:", elapsed)
