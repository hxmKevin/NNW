# -*- coding: utf-8 -*-
"""
该文件是爬取快递100的网页来获取快递信息
"""
import json
import time
import requests,random
from datetime import datetime

from common.JDcommon import returnData

flag = True

# 返回时间属于周几
def get_week_day(timestr):
    day = datetime.strptime(timestr, '%Y-%m-%d %H:%M:%S').weekday()

    week_day_dict = {
        0: '星期一',
        1: '星期二',
        2: '星期三',
        3: '星期四',
        4: '星期五',
        5: '星期六',
        6: '星期天',
    }
    return week_day_dict[day]

# 查询快递公司
def query_comCode(params):
    postid =params["postid"].replace(" ","")
    phone = params["phone"].replace(" ","")
    url = 'https://www.kuaidi100.com/autonumber/autoComNum'
    headers = {
        "origin": "https://www.kuaidi100.com",
        "referer": "https://www.kuaidi100.com/",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36 OPR/65.0.3467.78 (Edition Baidu)",
    }
    # 请求参数
    params = {
        'resultv2': '1',
        'text': postid
    }
    res = requests.get(url, params=params, headers=headers)
    result = False
    if res.status_code == 200:
        json_comCode = res.json()
        if 'auto' in json_comCode:
            if len(json_comCode['auto']) <= 0:
                print('快递公司识别错误！请检查快递单号是否输入正确！')
            else:
                for comCode in json_comCode['auto']:
                    # 查询快递进度
                    result = query_progress(comCode['comCode'], postid,phone)
                    if result:
                        break
        else:
            print('遇到错误了：{}'.format(json_comCode))

    else:
        print('查询快递公司：拒绝访问！')

    return result

# 查询快递进度
def query_progress(type = '', postid = '',phone = ''):
    url = "https://www.kuaidi100.com/query"
    headers = {
        "origin": "https://www.kuaidi100.com",
        "referer": "https://www.kuaidi100.com/",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36 OPR/65.0.3467.78 (Edition Baidu)",
    }
    # 请求参数
    params = {
        'type': type,
        'postid': postid,
        'temp': str(random.random()),
        'phone': phone
    }
    res = requests.get(url, params=params, headers=headers)
    if res.status_code == 200:
        json_data = res.json()
        # print(json_data)
        if json_data['status'] != '200':
            return False
        else:
            # print(json_data['data'])
            return json_data['data']
    else:
        print('查询快递进度：拒绝访问！')

# 打印查询记录
def print_result(result):
    # 倒叙
    result.reverse()

    result2 = []
    pre_week_str = ""
    for item in result:
        # 获取时间点是周几
        cur_week_str = get_week_day(item['ftime'])
        if pre_week_str != cur_week_str:
            pre_week_str = cur_week_str
            show_week_str = pre_week_str
        else:
            show_week_str = ""
        # 增加周几提示
        result2.append('{}{}：{}'.format(item['ftime'], " " + show_week_str, item['context']))

    # 倒叙
    result2.reverse()
    for item in result2:
        print(item)


def get_express_msg(params):
    print("express", type(params), params)
    try:
        data = query_comCode(params)
    except Exception as e:
        f = False
        msg = '查询快递信息失败,errmsg:{}'.format(e)
        errData = returnData(flag=f, msg=msg, backFillData=None)
        print(errData)
        return errData
    print(data)
    return json.dumps(data)



if __name__ == '__main__':
    print(datetime.now())
    params = {
                "postid" : "3103231024982",
                "phone" : ""
              }

    get_express_msg(params)
    print(datetime.now())
