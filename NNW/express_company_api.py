#coding:utf8
import json
from urllib.parse import quote
import requests
from flask import Flask, request
from NNW.kuai_di_niaoAPI import get_DataSign
EBusinessID = "1624721"#用户id
apiKey = "87436af5-4e06-4d72-81b2-d2245872cd37"#用户秘钥
app = Flask(__name__)

@app.route('/nnwemall/common/express_company_api',methods=['POST'])
def get_expressCompany():
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


if __name__ == '__main__':
    # requestData = {'LogisticCode':'3103231024982'}
    # get_expressCompany(requestData)
    app.run(host="0.0.0.0", port=5000, debug=True)