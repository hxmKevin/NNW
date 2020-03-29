#coding:utf8
import base64
import datetime
import hashlib
import json
from threading import Thread
from urllib.parse import quote
from flask import Flask, request
import requests
from common.handle_mongodb import HandleMongoDB
from pytz import timezone
m = HandleMongoDB()
mongoConnect = m.mongodb_connect()
app = Flask(__name__)
apiKey = "87436af5-4e06-4d72-81b2-d2245872cd37"#用户秘钥
RequestType = "8001"#请求类型
EBusinessID = "1624721"#用户id
cst_tz = timezone('Asia/Shanghai')
thread_count = 10#开启的线程数


def get_DataSign(requestData):
    """
    :param requestData: 请求数据
    :return: requestData + apiKey 先经过md5加密，再经过base64加密，得到DataSign
    """
    m = hashlib.md5()
    m.update((requestData + apiKey).encode("utf8"))
    encodestr = m.hexdigest()
    base64_text = base64.b64encode(encodestr.encode(encoding='utf-8')).decode("utf-8")
    return base64_text


def get_expressCompany(requestData):
    """获取对应快递单号的快递公司代码和名称"""

    url = "http://api.kdniao.com/Ebusiness/EbusinessOrderHandle.aspx"
    requestData = "{}".format(requestData)
    DataSign = get_DataSign(requestData)
    post_data = {
        'RequestData': quote(requestData),
        'EBusinessID': EBusinessID,
        'RequestType': '2002',
        'DataType': '2',
        'DataSign': quote(DataSign)
    }
    res = requests.post(url, post_data)
    # print(res.text)
    company = json.loads(res.text)["Shippers"][0]["ShipperCode"]
    # print(company)
    return company


def post_data(DataSign,requestData):
    url = "http://api.kdniao.com/Ebusiness/EbusinessOrderHandle.aspx"
    data = {
        "RequestData": quote(requestData),  # 数据内容(URL 编码:UTF-8)
        "EBusinessID": EBusinessID,  # 用户 ID
        "RequestType": RequestType,  # 请求指令类型
        "DataSign": quote(DataSign),  # 数据内容签名：把(请求内容(未编码)+ApiKey)进行 MD5 加密，然后 Base64编码，最后进行 URL(utf-8)编码
        "DataType": "2"  # (返回数据类型为 json)
    }
    # print(urlencode(data))
    # 进行url编码
    res = requests.post(url=url,data=data)
    return json.loads(res.text)




def run(requestDataList,thread_id):
    """
    :param requestData: str类型
    :return: 快递信息
    """
    idx = 1
    # print(requestDataList)
    for requestData in requestDataList:
        if idx % thread_count == thread_id:
            try:
                print("[ thread %s ]:start" % (str(thread_id).zfill(2)), "LogisticCode:", requestData["LogisticCode"])
                LogisticCode = requestData["LogisticCode"]  # 传入的快递单号
                ShipperCode_in = requestData["ShipperCode"]  # 传入的快递公司信息
                #1.先从mongodb中根据快递单号匹配快递公司数据，格式{"LogisticCode":ShipperCode,status:0}//status指快递单号和公司匹配是否存在问题
                cursor = mongoConnect["ExpressCompany"].find({"LogisticCode":LogisticCode})
                for item in cursor:
                    if item["status"] == 1:
                        datas.append({"LogisticCode":LogisticCode,"State":4,"msg":"数据库返回,该快递单号公司不匹配"})

                #2.如果没匹配到，则使用当前的传入数据
                requestData = "{}".format(requestData)
                DataSign = get_DataSign(requestData)
                data = post_data(DataSign, requestData)
                #如果无轨迹信息，进行判断是否快递信息出错
                if int(data["State"]) == 0:
                    #重新查询单号对应的快递公司，判断与传进来的快递公司是否匹配，如果不匹配则返回单号匹配错误
                    danhao_requestData = {"LogisticCode":LogisticCode}
                    ShipperCode_data = get_expressCompany(danhao_requestData)
                    if ShipperCode_data != ShipperCode_in:
                        time = cst_tz.localize(datetime.datetime(datetime.datetime.now().year, datetime.datetime.now().month, datetime.datetime.now().day,
                                                                 datetime.datetime.now().hour, datetime.datetime.now().minute, datetime.datetime.now().second))
                        mongoConnect["ExpressCompany"].insert({"LogisticCode":LogisticCode,"status":1,"time":time})
                        datas.append({"LogisticCode":LogisticCode,"State": 4, "msg": "经查询,该快递单号公司不匹配"})
                else:
                    Traces = data["Traces"]
                    print(Traces)
                    for item in Traces:
                        if item["Action"] == "0":
                            item["Action"] = "1"
                        elif item["Action"] == "1":
                            item["Action"] = "2"
                        elif item["Action"] in ["2" , "201"]:
                            item["Action"] ="3"
                        elif item["Action"] == "202" :
                            item["Action"] ="4"
                        elif item["Action"] in ["301" , "302" ,"304" , "311", '211']:
                            item["Action"] ="5"
                    print(data["Traces"])
                    datas.append(data)

            except Exception as e:
                datas.append({"LogisticCode": LogisticCode, "State": 4, "msg": e})


@app.route('/nnwemall/common/express_company_api',methods=['POST'])
def get_expressCompany_API():
    """获取对应快递单号的快递公司代码和名称"""
    requestData = request.get_data()
    requestData = json.loads(requestData.decode())
    url = "http://api.kdniao.com/Ebusiness/EbusinessOrderHandle.aspx"
    requestData = "{}".format(requestData)
    DataSign = get_DataSign(requestData)
    post_data = {
        'RequestData': quote(requestData),
        'EBusinessID': EBusinessID,
        'RequestType': '2002',
        'DataType': '2',
        'DataSign': quote(DataSign)
    }
    res = requests.post(url, post_data)
    if res.status_code == 200:
        data = json.loads(res.text)["Shippers"][0]
        result = {"code": 701, "messages": "操作成功", "data": data,}
    else:
        result = {"code": 601, "messages": "操作失败", "data": {"list": []}, }

    return json.dumps(result)

#开启多线程
def Multithreading(params):
    threads = list()
    print("All processes start...")
    for i in range(0, thread_count):  # 开启八进程,i作为线程号
        T = Thread(target=run, args=(params,i))  # 目标函数是run
        # Console.start("Process %s started..." % i)
        threads.append(T)
        T.start()

    for T in threads:  # 让主进程等待子进程结束才能结束
        T.join()

@app.route('/nnwemall/common/express',methods=['POST'])
def start():
    global datas
    datas = []
    requestDataList = request.get_data()
    requestDataList = json.loads(requestDataList.decode())
    Multithreading(requestDataList)
    print(datas)
    return json.dumps({"code": 701, "messages": "操作成功","data":datas})


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=80, debug=True)
    # requestData = [{'OrderCode':'','ShipperCode':'YD','LogisticCode':'3103231024982'}]
    # requestData = json.dumps(requestData).encode()
    # start(requestData)

    # run(requestData)
    # requestData = {'LogisticCode':'3103231024982'}
    # get_expressCompany(requestData)

