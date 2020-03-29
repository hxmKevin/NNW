# coding: utf-8
import base64
import datetime
import json
import os
import zipfile
import xlwt
from pytz import timezone
import xlrd
from datetime import date
from xlrd import open_workbook
from xlutils.copy import copy
from translate import Translator
import shutil
from NNW.order_stat import OrderStat
from common.handle_mongodb import HandleMongoDB
m = HandleMongoDB()
mongoConnect = m.mongodb_connect()
year = datetime.datetime.now().year
month = datetime.datetime.now().month
day = datetime.datetime.now().day
Weekday = datetime.datetime.now().weekday()
houtai_sta = 1250*5
sigu_sta = houtai_sta*2
cst_tz = timezone("Asia/Shanghai")
xlwt.add_palette_colour("custom_green_colour", 0x21)
style_column_bkg = xlwt.easyxf('pattern: pattern solid, fore_colour custom_green_colour; font: bold on;')
OrderTimeISODate = cst_tz.localize(datetime.datetime(year, month, day, 0, 0, 0))

def insertToMongo():
    x_list = [{'排名': 'Rank'}, {'名称': 'Name'}, {'订单数': 'Quantity_Ordered'}, {'累计订单数': 'Cumulative_orders'},
              {'销量': 'Sales_Volume'}, {'累计销量': 'Cumulative_sales'}, {'采购额': 'Purchases'},
              {'累积采购额': 'Cumulative_purchases'}, {'毛利': 'Gross_Margin'}, {'累计毛利': 'Accumulated_gross_margin'},
              {'营业额': 'Turnover'}, {'累计营业额': 'Cumulative_turnover'}, {'复购率/%': 'Repurchase_rate/%'},
              {'复购用户数': 'Number_of_users_repurchased'}, {'总用户数': 'Total_users'}, {'归属人': 'Who_belongs'},
              {'所属分组': 'Grouping'}, {'累积订单数': 'Cumulative_orders'}, {'累积销量': 'Cumulative_sales'},
              {'当日营业额': 'Turnover_of_the_day'}, {'累积营业额': 'Accumulated_turnover'},
              {'累积营业额(含退款)': 'Accumulated_turnover_(including_refunds)'}, {'南泥湾项目': 'South_Mud_Bay_Project'},
              {'职级': 'Position_Level'}, {'黄牌次数': 'Number_of_yellow_cards'}, {'分组': 'Group'}]
    name_dict = {"原分组": "Grouping2", "重复订单数": "Number_of_duplicate_orders", "订单复购率/%": "OrderRepeatR",
                 "新分组": "NewGroup", '总计': "Total", '排名': 'Rank', '名称': 'Name', '订单数': 'Quantity_Ordered',
                 '累计订单数': 'Cumulative_orders', '销量': 'Sales_Volume', '累计销量': 'Cumulative_sales', '采购额': 'Purchases',
                 '累积采购额': 'Cumulative_purchases', '毛利': 'Gross_Margin', '累计毛利': 'Accumulated_gross_margin',
                 '营业额': 'Turnover', '累计营业额': 'Cumulative_turnover', '复购率/%': 'Repurchase_rate',
                 '复购用户数': 'Number_of_users_repurchased', '总用户数': 'Total_users', '所属人': 'Who_belongs',
                 '归属人': 'Who_belongs', '所属分组': 'Grouping', '累积订单数': 'Cumulative_orders', '累积销量': 'Cumulative_sales',
                 '当日营业额': 'Turnover_of_the_day', '累积营业额': 'Accumulated_turnover',
                 '累积营业额(含退款)': 'Accumulated_turnover_including_refunds', '南泥湾项目': 'South_Mud_Bay_Project',
                 '职级': 'Position_Level', '黄牌次数': 'Number_of_yellow_cards', '分组': 'Group'}

    x1 = xlrd.open_workbook(path+'\\飞扬微商城统计{}月{}日.xls'.format(month,day))
    sheetList = x1.sheet_names()[2:]
    for sheetName in sheetList:
        # if sheetName != "轮休组个人":
        #     continue
        sheetObj = x1.sheet_by_name(sheetName)
        row_num = sheetObj.nrows
        for i in range(1, row_num):
            result = {}
            result["sheetName"] = sheetName
            result["date"] = OrderTimeISODate
            row_data = sheetObj.row_values(i)

            for index, key in enumerate(sheetObj.row_values(0)):
                x = {}
                key = name_dict[key]
                if key in ["Rank","Quantity_Ordered","Cumulative_orders","Sales_Volume","Cumulative_sales","Number_of_users_repurchased","Total_users","Number_of_yellow_cards"]:
                    try:
                        row_data[index] = int(row_data[index])
                    except:
                        if key == "Rank" :
                            row_data[index]=int(999)
                        elif key == "Number_of_yellow_cards":
                            row_data[index] = int(0)
                        else:
                            row_data[index] = ""
                elif key == "Repurchase_rate" and row_data[index]=="":
                    row_data[index] = float(0)
                elif key == "South_Mud_Bay_Project" and row_data[index]=="":
                    row_data[index] = float(0)

                # 更新黄牌使用
                # if sheetName=="个人统计"and key == "Number_of_yellow_cards" and result["South_Mud_Bay_Project"]==2.0 and Weekday==6:
                #     name = result["Who_belongs"]
                #     cursor = mongoConnect["NNWIdName"].find({"RealName":name})
                #     for item in cursor:
                #         upgroup = item["UpperGroup"]
                #         if item["VersionTwoGroup"]["GroupLeader"] == 3:
                #             houtai_sta = houtai_sta*0.75
                #         break
                #     if upgroup == "后台组" :
                #         if result["Accumulated_turnover_including_refunds"] < houtai_sta:
                #             # result["Number_of_yellow_cards"] +=1
                #             mongoConnect["NNWIdName"].update_one({"RealName":name},{"$inc":{"YellowCardTimesCurrent":1}})
                #             mongoConnect["NNWIdName"].update_one({"RealName": name}, {"$inc": {"YellowCardTimes": 1}})
                #         else:
                #             mongoConnect["NNWIdName"].update_one({"RealName": name},{"$set": {"YellowCardTimesCurrent": 0}})
                #
                #     elif upgroup =="私顾组":
                #         if result["Accumulated_turnover_including_refunds"] < sigu_sta:
                #             # result["Number_of_yellow_cards"] +=1
                #             mongoConnect["NNWIdName"].update_one({"RealName":name},{"$inc":{"YellowCardTimesCurrent":1}})
                #             mongoConnect["NNWIdName"].update_one({"RealName": name}, {"$inc": {"YellowCardTimes": 1}})
                #         else:
                #             # result["Number_of_yellow_cards"] = 0
                #             mongoConnect["NNWIdName"].update_one({"RealName": name},{"$set": {"YellowCardTimesCurrent": 0}})


                result[key] = row_data[index]
            mongoConnect["NNWStatistic"].insert(result)

def Summation():
    x1 = xlrd.open_workbook("./order.xls")
    sheetList = x1.sheet_names()
    #读取excel
    result = {}
    d = 0
    for sheetName in sheetList:
        if sheetName == "详细导出":
            d += 1
            continue
        result[d] = []
        sheetObj = x1.sheet_by_name(sheetName)
        col_num = sheetObj.ncols
        row_num = sheetObj.nrows
        for i in range(1, col_num):
            col_data = sheetObj.col_values(i)
            if i == 1:
                result[d].append({
                    "row":row_num,
                    "col":1,
                    "value":"总计"
                })
                # sheetObj.write(len(col_data)+1,1,"总计")
            else:
                title = col_data[0]
                if title not in ["所属分组","原分组","南泥湾项目","职级","分组","订单ID","下单时间","归属人","产品名称","所属组","详细名称",
                                 ]:
                    sums = 0
                    for s in col_data[1:]:
                        s = 0 if s=="" else s
                        sums += s
                    result[d].append({
                        "row": row_num,
                        "col": i,
                        "value": sums
                    })
                    # sheetObj.write(len(col_data) + 1, i, sums)
        d +=1

    # 打开想要更改的excel文件
    old_excel = xlrd.open_workbook('order.xls', formatting_info=True)
    # 将操作文件对象拷贝，变成可写的workbook对象
    new_excel = copy(old_excel)
    # 获得第一个sheet的对象
    for i in range(0,14):
        if i == 1:
            continue
        ws = new_excel.get_sheet(i)
        # 写入数据
        result_list = result[i]
        for i in result_list:
            ws.write(i["row"], i["col"],i["value"])

    new_excel.save(path+'\\飞扬微商城统计{}月{}日.xls'.format(month,day))

def get_excels_订单核心统计():
    old_excel = xlrd.open_workbook(f_path)
    sheetList = old_excel.sheet_names()
    result = []
    for sheetName in sheetList:
        if sheetName == "订单核心统计":
            sheetObj = old_excel.sheet_by_name(sheetName)
            row_num = sheetObj.nrows
            for i in range(1, row_num):
                row_data = sheetObj.row_values(i)
                result.append(row_data)
                # print(len(row_data))

    workbook = xlwt.Workbook(encoding='utf-8')
    worksheet = workbook.add_sheet('订单核心统计')
    workbook.set_colour_RGB(0x21, 169, 208, 142)
    column_name = [{"name": "订单ID", "width": 10}, {"name": "下单时间", "width": 20}, {"name": "归属人", "width": 10},
                   {"name": "所属组", "width": 10}, {"name": "产品名称", "width": 30}, {"name": "详细名称", "width": 20},
                   {"name": "销售数", "width": 10}, {"name": "销售单价", "width": 12}, {"name": "采购单价", "width": 12},
                   {"name": "销售金额", "width": 12}, {"name": "收款金额", "width": 12}, {"name": "收款额成本", "width": 12},
                   {"name": "毛利", "width": 12}]

    i = 0
    for column in column_name:
        worksheet.write(0, i, column['name'], style_column_bkg)
        first_col = worksheet.col(i)
        first_col.width = 256 * column['width']
        i += 1


    row = 1
    for i in result:
        col = 0
        for x in i:
            worksheet.write(row,col,x)
            col+=1
        row +=1
    workbook.save(path+'\\飞扬微商城订单核心统计{}月{}日.xls'.format(month, day))

def get_excels_7日统计():
    old_excel = xlrd.open_workbook(f_path)
    sheetList = old_excel.sheet_names()
    result = []
    for sheetName in sheetList:
        if sheetName == "近七日产品统计":
            sheetObj = old_excel.sheet_by_name(sheetName)
            row_num = sheetObj.nrows
            for i in range(1, row_num):
                row_data = sheetObj.row_values(i)
                result.append(row_data)
                # print(len(row_data))


    workbook = xlwt.Workbook(encoding='utf-8')
    worksheet = workbook.add_sheet('近七日产品统计')
    workbook.set_colour_RGB(0x21, 169, 208, 142)
    column_name = [
        {"name": "排名", "width": 6}, {"name": "名称", "width": 20}, {"name": "订单数", "width": 10},
        {"name": "累计订单数", "width": 12}, {"name": "销量", "width": 10}, {"name": "累计销量", "width": 12},
        {"name": "采购额", "width": 12}, {"name": "累积采购额", "width": 15}, {"name": "毛利", "width": 12},
        {"name": "累计毛利", "width": 12}, {"name": "营业额", "width": 12}, {"name": "累计营业额", "width": 15},
        {"name": "复购率/%", "width": 12}, {"name": "订单复购率/%", "width": 12}, {"name": "复购用户数", "width": 12},
        {"name": "总用户数", "width": 12}, {"name": "重复订单数", "width": 12}
    ]

    i = 0
    for column in column_name:
        worksheet.write(0, i, column['name'], style_column_bkg)
        first_col = worksheet.col(i)
        first_col.width = 256 * column['width']
        i += 1


    row = 1
    for i in result:
        col = 0
        for x in i:
            worksheet.write(row,col,x)
            col+=1
        row +=1
    workbook.save(path+'\\飞扬微商城7日统计{}月{}日.xls'.format(month, day))

def get_excels_联创():
    workbook = xlwt.Workbook(encoding='utf-8')
    old_excel = xlrd.open_workbook(f_path)
    sheetList = old_excel.sheet_names()

    for sheetName in sheetList:
        result = []
        if sheetName in ["个人统计(联创)","分组统计(联创)"]:
            sheetObj = old_excel.sheet_by_name(sheetName)
            row_num = sheetObj.nrows
            for i in range(1, row_num):
                row_data = sheetObj.row_values(i)
                result.append(row_data)


            worksheet = workbook.add_sheet(sheetName)
            workbook.set_colour_RGB(0x21, 169, 208, 142)
            if sheetName == "个人统计(联创)":
                column_name = [
                    {'name': '排名', 'width': 6},
                    {'name': '归属人', 'width': 10},
                    {'name': '所属分组', 'width': 10},
                    {'name': '原分组', 'width': 10},
                    {'name': '订单数', 'width': 10},
                    {'name': '累积订单数', 'width': 13},
                    {'name': '销量', 'width': 10},
                    {'name': '累积销量', 'width': 13},
                    {'name': '当日营业额', 'width': 13},
                    {'name': '累积营业额', 'width': 13},
                    {'name': '累积营业额(含退款)', 'width': 18},
                    {'name': '南泥湾项目', 'width': 13},
                    {'name': '职级', 'width': 10},
                    {'name': '黄牌次数', 'width': 10},
                ]

                i = 0
                for column in column_name:
                    worksheet.write(0, i, column['name'], style_column_bkg)
                    first_col = worksheet.col(i)
                    first_col.width = 256 * column['width']
                    i += 1
            else:
                column_name = [
                    {'name': '排名', 'width': 6},
                    {'name': '分组', 'width': 10},
                    {'name': '订单数', 'width': 10},
                    {'name': '累积订单数', 'width': 13},
                    {'name': '销量', 'width': 10},
                    {'name': '累积销量', 'width': 10},
                    {'name': '当日营业额', 'width': 13},
                    {'name': '累积营业额', 'width': 13},
                ]

                i = 0
                for column in column_name:
                    worksheet.write(0, i, column['name'], style_column_bkg)
                    first_col = worksheet.col(i)
                    first_col.width = 256 * column['width']
                    i += 1


            #处理数据
            row = 1
            for i in result:
                col = 0
                for x in i:
                    worksheet.write(row,col,x)
                    col+=1
                row +=1
    workbook.save(path +'\\飞扬微商城联创统计{}月{}日.xls'.format(month, day))

def get_详细导出():
    old_excel = xlrd.open_workbook('order.xls')
    sheetList = old_excel.sheet_names()
    result = []
    for sheetName in sheetList:
        if sheetName == "详细导出":
            sheetObj = old_excel.sheet_by_name(sheetName)
            row_num = sheetObj.nrows
            for i in range(1, row_num):
                row_data = sheetObj.row_values(i)
                result.append(row_data)
                # print(len(row_data))

    workbook = xlwt.Workbook(encoding='utf-8')
    worksheet = workbook.add_sheet('订单核心统计')
    workbook.set_colour_RGB(0x21, 169, 208, 142)
    column_name = ["订单ID", "备注标签", "备注", "用户备注标签", "用户备注", "供应商备注标签", "供应商备注", "产品ID", "SKU名称",
                   "发送费", "采购系统发送费", "单价", "总金额", "每份返现金额", "返现总金额", "采购价", "采购总额", "零售价格", "零售总金额", "利润", "总利润",
                   "零售利润", "零售总利润", "总数", "可使用数", "已使用数", "申请退单数", "已退单数", "供应商ID", "供应商",
                   "供应商分组", "联系人", "联系人手机号", "联系人身份证号", "游客姓名", "游客手机号", "游客身份证号", "产品名称",
                   "下单时间", "更新时间", "快递地址", "系统备注", "佣金比例", "联票佣金比例", "产品分类", "分享员账号", "分享员ID", "采购编码",
                   "游玩时间段", "接送地址", "附加成本", "推广成本", "发票信息", "预约信息", "快递单号", "销量", "营业额", "营业额成本", "毛利", "下单人id",
                   "归属人", "归属人id",
                   "归属人分组",
                   "订单数"]

    i = 0
    for column in column_name:
        worksheet.write(0, i, column, style_column_bkg)
        i += 1

    row = 1
    for i in result:
        col = 0
        for x in i:
            worksheet.write(row, col, x)
            col += 1
        row += 1
    workbook.save('C:\\Users\\80530\\Desktop\\python_work\\NNW\\飞扬发货单{}年{}月{}日\\详细导出.xls'.format(year,month, day))

#压缩文件
def zip_ya(start_dir,after_dir):
    start_dir = start_dir  # 要压缩的文件夹路径
    file_news = after_dir + '.zip'  # 压缩后文件夹的名字
    z = zipfile.ZipFile(file_news, 'w', zipfile.ZIP_DEFLATED)
    for dir_path, dir_names, file_names in os.walk(start_dir):
        f_path = dir_path.replace(start_dir, '')  # 这一句很重要，不replace的话，就从根目录开始复制
        f_path = f_path and f_path + os.sep or ''  # 实现当前文件夹以及包含的所有文件的压缩
        for filename in file_names:
            z.write(os.path.join(dir_path, filename), f_path + filename)
    z.close()

#发送文件到微信
def send_msg():
    pass



if __name__ == '__main__':
    path = 'C:\\Users\\80530\\Desktop\\飞扬微商城{}月{}日'.format(month, day)
    f_path = path + '\\飞扬微商城统计{}月{}日.xls'.format(month, day)
    isExists = os.path.exists(path)
    # 判断结果
    if not isExists:
        os.makedirs(path)
    #生成表格
    OS = OrderStat()
    OS.main()#生成order表
    if datetime.datetime.now().hour == 17:
        Summation()#加入总计
        get_excels_订单核心统计()#生成单独的核心订单统计表
        get_excels_7日统计()#生成单独的7日统计表
        get_excels_联创()#生成单独的联创表
        insertToMongo()#将数据插入到数据库中
    else:
        #需要压缩的文件
        start_dir = "C:\\Users\\80530\\Desktop\\python_work\\NNW\\飞扬发货单{}年{}月{}日".format(year,month,day)
        #压缩后的文件
        after_dir = "C:\\Users\\80530\\Desktop\\飞扬微商城{}月{}日\\飞扬发货单{}年{}月{}日".format(month,day,year,month,day)
        get_详细导出()
        zip_ya(start_dir, after_dir)