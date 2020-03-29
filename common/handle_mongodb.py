# coding: utf-8

import pymongo
from common.readconfig import ReadConfig


class HandleMongoDB:
    def __init__(self):
        self.data = ReadConfig()

    def mongodb_connect(self):
        """连接数据库"""
        mongodb_ip = str(self.data.get_db("mongodb_ip"))
        mongodb_port = int(self.data.get_db("mongodb_port"))
        mongodb_auth = str(self.data.get_db("mongodb_auth"))
        mongodb_password = str(self.data.get_db("mongodb_password"))

        # if mongodb_ip != '192.168.88.21':
        #     print("caution: you are connecting non-test mongodb_ip : " + mongodb_ip)
        #     n = 'none'
        #     while n != 'y' and n != 'n':
        #         n = input('Do you want to continue y/n : ')
        #     if n == 'y':
        #         pass
        #     else:
        #         print('Program terminated')
        #         exit()

        client = pymongo.MongoClient(mongodb_ip, mongodb_port)
        self.mongodb = client['erp']
        self.mongodb.authenticate(mongodb_auth, mongodb_password)

        return self.mongodb

    def select_col(self, col):
        return self.mongodb[str(col)]


if __name__ == '__main__':
    pass
    # test = HandleMysql()
    # sql = "select * from maoyan_movie"
    # for i in test.search(sql):
    #     print(i)
