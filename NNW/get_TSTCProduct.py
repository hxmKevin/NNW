#coding:utf-8
"""
从天时同城将产品数据导入到mongodb中
"""
import datetime
import requests
import xlrd
from pytz import timezone
cst_tz = timezone("Asia/Shanghai")
from common.handle_mongodb import HandleMongoDB
from lxml import etree
m = HandleMongoDB()
mongoConnect = m.mongodb_connect()
def step1():
    x1 = xlrd.open_workbook("./test.xlsx")
    sheetObj = x1.sheet_by_name("Sheet3")
    row_num = sheetObj.nrows
    for i in range(1, row_num):
        row_data = sheetObj.row_values(i)
        data = {
            "TSID":int(row_data[0]),
            "row":i,
            "CreatTime":"",
            "UpdateTime":"",
            "Name":row_data[1],
            "Title":row_data[2],
            "SupplierID": row_data[5],
            "Supplier": row_data[6],
            "分销价": row_data[7],
            "采购价": row_data[8],
            "指导价": row_data[9],
            "门市价": row_data[10],
            "排序": row_data[11],
            "状态": row_data[12],
            "退单类型": row_data[13],
            "退单手续费": row_data[14],
            "退单限制时间":row_data[15],
            "码号类型":row_data[16],
            "标题模板":row_data[17],
            "内容模板": row_data[18],
            "购买须知": row_data[19],
            "简介": row_data[20],
            "有效日期": "",
        }
        mongoConnect["TSTC_product"].insert(data)
        print(data)

def step2():
    htmlf = open('./test.htm', 'r', encoding="utf-8")
    htmlcont = htmlf.read()
    html = etree.HTML(htmlcont)
    k_list = []
    for i in range(2, 134):
        x_list = []
        for x in range(1, 5):
            tr = html.xpath("//tr[{}]/td[{}]/text()".format(i, x))
            t = tr[0]
            x_list.append(t)
        # print(x_list)
        k_list.append(x_list)
    for i in k_list:
        id = int(i[0])
        creatTime = i[1]
        updateTime = i[2]
        creatHour = int(creatTime.split(" ")[1].split(":")[0])
        updateHour = int(updateTime.split(" ")[1].split(":")[0])
        creatTime = cst_tz.localize(datetime.datetime(2020, int(creatTime.split("/")[1]), int(creatTime.split("/")[2].split(" ")[0]),creatHour, 0, 0))
        updateTime = cst_tz.localize(datetime.datetime(2020, int(updateTime.split("/")[1]), int(updateTime.split("/")[2].split(" ")[0]), updateHour, 0, 0))
        # print(creatTime)
        # print(updateTime)
        mongoConnect["TSTC_product"].update_one({"TSID":id},{"$set":{"CreatTime":creatTime,"UpdateTime":updateTime,"有效日期":i[3]}})


if __name__ == '__main__':
    # step1()
    step2()

