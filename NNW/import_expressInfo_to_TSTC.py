# coding: utf-8
"""
将数据的订单信息导入到天时同城中去，一次只能导入200条
"""
import time
import requests
import logging
import json
from common.handle_mongodb import HandleMongoDB
from datetime import datetime
logging.basicConfig(filename='TSTC_ExpressInfo.log', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


class Import_ExpressInfo_to_TSTC(object):
    def __init__(self):
        m = HandleMongoDB()
        self.mongoConnect = m.mongodb_connect()
        self.loger = logging.getLogger()
        self.zj_user_name = "zjfeiyang"
        self.zj_user_pwd = "121188a"
        self.PHPSESSID = "bd8b955f0870d5b62f56a56f95490c4247ac"
# 2.连接数据库获取需要导入的订单号信息
    def login(self):
        print('start login')
        url = 'http://fenxiao.feiyang.cn/Admin/Index/login.json'
        data = {'account': self.zj_user_name, 'password': self.zj_user_pwd}

        session = requests.session()

        req_header = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36',
        }

        # 使用session发起请求
        response = session.post(url, headers=req_header, data=data)
        # print(response.text)

        if response.status_code == 200:

            url = 'http://fenxiao.feiyang.cn/Admin/Index/?login=success'

            response = session.get(url, headers=req_header)

            if response.status_code == 200:
                d = session.cookies.get_dict()
                self.PHPSESSID = d['PHPSESSID']
                self.loger.info("更改PHPSESSID:{}".format(self.PHPSESSID))



    # 1.每五分钟导入一次，一次导入最多200条，剩下的下一批再导入。
    def import_expressInfo(self):
        cursor = self.mongoConnect["NNWOrder"].find({"kuai4_di4_dan1_hao4": {"$nin": [""," "]},"SyncTSExpressStatus":1}).limit(200)
        nums = ""
        for item in cursor:
            orderNumber = item["ID"]
            postNum = item["kuai4_di4_dan1_hao4"]
            nums+= orderNumber+" "+postNum+"\n"
        print(nums)
        return nums

    def postData(self,nums):
        if nums == "":
            self.loger.info("无待导入快递号:{}".format(nums))
            return
        self.loger.info("待导入快递号:{}".format(nums))
        url = "http://fenxiao.feiyang.cn/Admin/Orders/importOrdersPostNo.json?connect_redirect=0&_r=0.05373215675354"
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cache-Control": "max-age=0",
            "Connection": "keep-alive",
            "Content-Length": "209",
            "Cookie": "PHPSESSID={}".format(self.PHPSESSID),
            "Host": "fenxiao.feiyang.cn",
            "Origin": "http://fenxiao.feiyang.cn",
            "Referer": "http://fenxiao.feiyang.cn/Admin/Orders/importOrdersPostNo.html",
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36",
            'X-Forwarded-For':"172.1.23.32"
        }

        files = {"orderspostno":(None,nums)}
        # files = {"orderspostno": (None, )}
        res = requests.post(url=url, headers=headers, files=files, verify=False)
        print(json.loads(res.text))

        msg = json.loads(res.text)["message"]
        if msg == "用户登录过期":
            print("用户登录过期")
            self.login()
        elif msg == "有部分信息操作失败" or "成功":
            self.loger.info("快递号导入成功:{}".format(nums))
            numList = nums.split("\n")
            for i in numList:
                if i !="":
                    id = i.split(" ")[0]
                    self.mongoConnect["NNWOrder"].update_one({"ID": id}, {"$set": {"SyncTSExpressStatus": 2}})
            print("修改SyncTSExpressStatus成功")
            self.loger.info("修改SyncTSExpressStatus成功:{}".format(nums))
    def run(self):
        for i in range(1,99999999):
            if i%200 == 0:
                self.login()
            nums = self.import_expressInfo()
            self.postData(nums)
            time.sleep(60)



def import_entrance():
    ex = Import_ExpressInfo_to_TSTC()
    ex.loger.info("导入快递单号程序启动。。。")
    ex.run()

# 3.爬虫将订单号导入到订单中，返回导入成功信息
# 4.将导入的数据库订单信息做标记
if __name__ == '__main__':
    for i in range(1000):
        try:
            import_entrance()
        except:
            pass
