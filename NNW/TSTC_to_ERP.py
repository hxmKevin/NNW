#coding:utf-8
"""
该脚本是将天时同城的数据导入到erp中
"""
from numpy.core.defchararray import zfill
from pytz import timezone
import datetime
import xlrd
from bson import ObjectId
cst_tz = timezone("Asia/Shanghai")
year = datetime.datetime.now().year
month = datetime.datetime.now().month
day = datetime.datetime.now().day
hour = datetime.datetime.now().hour
from common.handle_mongodb import HandleMongoDB
m = HandleMongoDB()
mongoConnect = m.mongodb_connect()



def product():
    """
    从天时同城中导入产品表
    :return:
    """
    cursor = mongoConnect["TSTC_product"].find().batch_size(30)
    n = 23
    n = str(n).zfill(9)
    for item in cursor:
        n+=1
        SupplierName = item["Supplier"]
        s = mongoConnect["MallSupplier"].find({"SupplierName":SupplierName})
        for item_s in s:
            ForeignKeyID = item_s["_id"]
        product_data = {
            "ProductName" : item["Name"],#产品名称
            "ProductTitle" : item["Title"],#产品标题
            "ProductNO" : "MP"+ n,#产品编号，自动生成9位
            "Status" : 1 if item["状态"] == "正常" else 0,#状态，1上架，0下架
            "Sort" : item["排序"],#排序字段，小的排前面
            "RefundType" : 1,#退单类型，0禁止退单，1允许退单
            "BuyReading" : item["购买须知"],#购买须知
            "ProductIntroduction" : item["简介"],#产品简介
            "ProductDetail" : "<p>\n\t产品详情\n</p>\n<p>\n\t<br />\n</p>\n<p>\n\t<br />\n</p>",#产品详情？？？？？？？？？？
            "DefaultPic" : "http://img1.iflying.com/prd/202003/17/20200317135948988.jpg",#默认照片？？？？？？？？？？
            "ShareDescribe" : "分享描述",#分享描述？？？？？？？？？？
            "ShareCopywriting" : "分享文案",#分享文案？？？？？？？？？？
            "ProductGroup" : {#产品所属类别？？？？？？？？？？TODO
                "ForeignKeyID" : ObjectId("5e609fb6a5c656341ce85315"),
                "Name" : "零食"
            },
            "TodayMinPrice" : [#今日各个分销商每个会员等级最小价格，用于列表接口价格展示

            ],
            "IsDel" : 0.0,#是否删除
            "Owner" : {#产品拥有者
                "ForeignKeyID" : ObjectId("5c875ec3b0ae1c1445df953f"),
                "EmployeeName" : "钱凯怡",
                "EmployeeDepartmentID" : ObjectId("000000000000000000000043"),
                "EmployeeDepartmentName" : "企划部"
            },
            "Company" : {#所属公司
                "ForeignKeyID" : ObjectId("000000000000000000000001"),
                "DelStatus" : int(0),
                "SqlServerID" : int(0),
                "CompanyName" : "浙江飞扬国际旅游集团股份有限公司",
                "ParentCompanyID" : ObjectId("000000000000000000000000"),
                "CompanyNameSimplifiedSpelling" : "",
                "CompanyDefaultContact" : "何斌锋",
                "CompanyDefaultContactMobile" : "0574-27666666",
                "CompanyStatus" : int(1)
            },
            "CreateTime" : item["CreatTime"],
            "UpdateTime" : item["UpdateTime"],
            "CreateUser" : {#是否能固定
                "ForeignKeyID" : ObjectId("5c875ec3b0ae1c1445df953f"),
                "EmployeeName" : "钱凯怡",
                "EmployeeDepartmentID" : ObjectId("000000000000000000000043"),
                "EmployeeDepartmentName" : "企划部"
            },
            "UpdateUser" : {#是否能固定
                "ForeignKeyID" : ObjectId("5c875ec3b0ae1c1445df953f"),
                "EmployeeName" : "钱凯怡",
                "EmployeeDepartmentID" : ObjectId("000000000000000000000043"),
                "EmployeeDepartmentName" : "企划部"
            },
            "Supplier" : {#供应商
                "ForeignKeyID" : ForeignKeyID,
                "SupplierName" : SupplierName
            },
            "Express" : {#
                "IsMaterialObject" : 1,#是否实体，默认1
                "CityDistribution" : {#同城配送
                    "Is" : 0,#是否
                    "Cost" : 9.99#价格
                },
                "Express" : {
                    "Is" : 1,#是否
                    "Cost" : 11.1,#价格
                    "FreeLine" : 80,#满多少包邮
                    "IsExpressInclude" : 0,#判断是取包含还是不包含 TODO
                    "Include" : [
                        {
                            "ID" : ObjectId("000000000000000000000037"),
                            "Title" : "云南"
                        }
                    ],
                    "Exclude" : [

                    ]
                }
            },
            "SellingPoint" : item["简介"]#卖点
        }
        #将自动生成编号的表的值加1
        mongoConnect["IncrementID"].update_one({"Name":"MallProduct"},{"$set":{"Value":n+2}})

        mongoConnect["MallProduct"].insert(product_data)

        print(product_data)
        break

def order():
    cursor_order = mongoConnect["NNWOrder"].find().batch_size(30)
    for item in cursor_order:
        #根据SKU绑定产品编号
        cursor = mongoConnect["MallProduct"].find({"ProductTitle":item["SKU_ming2_cheng1"]})
        for item_2 in cursor:
            ProductNo = item_2["ProductNO"]
            ProductID = item_2["_id"]
        order_moudle={
            "OrderNo": item["ID"],#订单编号，是不是自动生成？
            "ProductNo": ProductNo,#产品编号,自动生成
            "ProductID": ProductID,#产品ID,产品表中的id
            "ShorterName": item["ming2_cheng1"],#产品名称（简称productName）
            "FullName": item["SKU_ming2_cheng1"],#产品详细名称productTitle
            "StockID": "ObjectId",#库存ID?????????????
            "StockPriceID": "ObjectId",#库存价格ID?????????????
            "PackageID": "ObjectId",#?????????????
            "PackageName": item["SKU_ming2_cheng1"],#SKU
            "ChannelID": item["gong1_ying4_shang1_ID"],#渠道ID
            "ChannelName": item["gong1_ying4_shang1"],#渠道名称
            "Customer": {#客户信息
                "CustomerName": item["you2_ke4_xing4_ming2"],#客户名称
                "CustomerID": item["OrderCustomerId"],#客户ID
                "NickName": item["OrderCustomerName"],#用户昵称
                "AccountName": "",#用户账号
                "OpenId": "",#用户openid
                "Mobile": item["you2_ke4_shou3_ji1_hao4"],#用户手机号
                "Address": item["kuai4_di4_di4_zhi3"],#用户地址
                "AddressArea": {#用户地址区域
                    "Province": "",#省
                    "City": "",#市
                    "County": "",#区
                    "Street": "",#街道
                },#有些地址不完整，沒法進行提取省市區

            },
            "DeliveryExpress": {
                "ExpressName": item["ExpressCompanyName"] if "ExpressCompanyName" in item else "",#快递名称
                "ExpressNo": item["ExpressNumber"] if "ExpressNumber" in item else "",#快递单号
                "DeliveryName": item["lian2_xi4_ren2"],#收货人名称
                "DeliveryID": "",#收货人地址ID
                "Mobile": item["lian2_xi4_ren2_shou3_ji1_hao4"],#收货人手机
                "Address": item["kuai4_di4_di4_zhi3"],#收货人详细地址
                "AddressArea": {#用户地址区域
                    "Province": "",#省
                    "City": "",#市
                    "County": "",#区
                    "Street": "",#街道
                },

            },
            "TotalMoney": item["zong3_jin1_e2"],#产品总金额
            "TotalNumber": item["SalesVolume"],#产品数量
            "SellUnitMoney": item["dan1_jia4"],#销售单价
            "DiscountID": "",#优惠ID
            "DiscountMoney": "",#优惠金额
            "RealTotalMoney": item["BusinessTurnover"],#订单最终金额（产品价格-优惠价格+运费）
            "FreightMoney": "",#运费
            "RefundMoney": item["zong3_jin1_e2"] - item["BusinessTurnover"],#退款金额
            "CostMoney": item["BusinessTurnoverCost"],#成本
            "PayTime": item["OrderTimeISODate"],#付款时间
            "SerialNumber": item["zhi1_fu4_liu2_shui3_hao4"],#流水号
            "OrderStatus": 3 if item["yi3_tui4_dan1_shu4"] >=1 else 1,#订单状态1正常、2取消、3退单（关闭）
            "PayStatus": item["PayStatus"],#收款状态（PayStatus）1未收款2已收款
            "IsAllowRefund": "",#是否允许退款
            "ExpectDeliveryTime": None,#手动选择的预计发货时间
            "AutoExpectDeliveryTime": None,#系统自动计算的预计发货时间
            "Delivery": {#发货信息
                "ImportID": "",#导入ID
                "CreateTime": item["UpdateTime"],#添加时间
                "DeliveryUser": {#发货人
                    "UserID": "",  # 发货人ID
                    "CompangID": "",  # 发货人所属公司（暂时不用）
                    "UserName": "",  # 发货人姓名
                    "GroupId": "",  # 发货人组ID
                    "GroupName": "",  # 发货人组名称
                    "GroupPost": "",  # 发货人组职位
                },
                "ExpressCustomerName": item["lian2_xi4_ren2"],#收货人名称
                "ExpressMobile": item["lian2_xi4_ren2_shou3_ji1_hao4"],#收货人手机号
                "ExpressName": item["ExpressCompanyName"] if "ExpressCompanyName" in item else "",#快递名称
                "ExpressNo":item["ExpressNumber"] if "ExpressNumber" in item else "",#快递号
                "DeliverAddress": item["kuai4_di4_di4_zhi3"],#收货人地址
                "DestinationAddress": item["kuai4_di4_di4_zhi3"],#收货人地址区域
                "DeliveryTime": item["UpdateTime"],#发货时间
                "DeliveryStatus": 2 if item["SalesVolume"] > 0 else 1,#发货状态（DeliveryStatus）1未发货2已发货
                "Notes": item["yong4_hu4_bei4_zhu4"],#备注
            },
            "OwnUser": {
                "UserID": item["BelongId"],#销售ID
                "CompangID": "",#销售所属公司（暂时不用）
                "UserName": item["BelongName"],#销售姓名
                "GroupId": item["BelongGroupId"],#组ID
                "GroupName": item["BelongGroup"],#组名称
                "GroupPost": "",#组职位
            },#销售
            "Profit": item["GrossProfit"],#毛利
            "Notes": item["bei4_zhu4"],#备注

        }
        mongoConnect["ShoppingOrder"].insert(order_moudle)
        break

def stock():
    cursor = mongoConnect["MallProduct"].find().batch_size(30)
    for item_product in cursor:
        SallCount = mongoConnect["NNWOrder"].find({"SKU_ming2_cheng1":item_product["ProductTitle"]}).count()
        stocks = {
            "ProductId" : item_product["_id"], #产品id
            "SKU" : item_product["ProductTitle"], #SKU  TODO：获取产品的属性，"甜度-5_包装-300ml_品牌-测试"
            "SKUPresent" : item_product["ProductTitle"], #当前SKU
            "StockReduceType" : {#优惠类型，固定
                "Id" : 1,
                "Name" : "下单减"
            },
            "PresentRealStockStatus" : 1, #固定
            "DailyDeliverStatus" : 0,#每日限购 1：有 ，0:没有 TODO:后期手动修改
            "UpdateUser" : {#更新人员
                "ForeignKeyID" : ObjectId("000000000000000000002251"),
                "EmployeeName" : "系统管理员Online",
                "EmployeeDepartmentID" : ObjectId("000000000000000000000781"),
                "EmployeeDepartmentName" : "浙江恒越信息科技有限公司"
            },
            "UpdateTime" :cst_tz.localize(datetime.datetime(year,month,day, hour, 0, 0)),
            "SallCount" : SallCount,#已售数？？？？？？
            "StockCount" : 99999,#剩余库存数量  TODO：暂时写99999,后期进行手动修改
            "StockCountAll" : 99999,#库存总数 = 已售数 +剩余库存数量
            "AddUser" : {#添加人员
                "ForeignKeyID" : ObjectId("000000000000000000002251"),
                "EmployeeName" : "系统管理员Online",
                "EmployeeDepartmentID" : ObjectId("000000000000000000000781"),
                "EmployeeDepartmentName" : "浙江恒越信息科技有限公司"
            },
            "BelongUser" : {#属于人员，运营
                "ForeignKeyID" : ObjectId("000000000000000000002251"),
                "EmployeeName" : "系统管理员Online",
                "EmployeeDepartmentID" : ObjectId("000000000000000000000781"),
                "EmployeeDepartmentName" : "浙江恒越信息科技有限公司"
            },
            "AddTime" : cst_tz.localize(datetime.datetime(year,month,day, hour, 0, 0)),#增加时间
            "DelStatus" : 0#删除状态
        }

        #将数据插入到MallStock表中
        # mongoConnect["MallStock"].insert(stocks)

        cursor = mongoConnect["MallStock"].find({"ProductId":item_product["_id"]})

        for item_stock in cursor:
            #获取价格
            price_cursor = mongoConnect["TSTC_product"].find({"Title":item_product["ProductTitle"]})
            for price_item in price_cursor:
                Price = price_item["门市价"]
                Cost = price_item["采购价"]
                PricePresent = price_item["门市价"]
            stockPrice = {
                "StockId" : item_stock["_id"],#库存id
                "ProductId" : item_product["_id"],#产品id
                "StockType" : {#库存类型暂时不动
                    "Id" : 1,
                    "Name" : "共享库存"
                },
                "Price" : Price,#销售价格
                "PricePresent" : PricePresent,#当前价格，划线价，没有的话目前同售价  TODO:划线价
                "Cost" : Cost,#成本
                "ChannelInfo" : {#渠道，固定
                    "ChannelId" : ObjectId("5e65ec91e0199723e09a0537"),
                    "ChannelName" : "南泥湾"
                },
                "UpdateUser" : {#更新人员
                    "ForeignKeyID" : ObjectId("000000000000000000002251"),
                    "EmployeeName" : "系统管理员Online",
                    "EmployeeDepartmentID" : ObjectId("000000000000000000000781"),
                    "EmployeeDepartmentName" : "浙江恒越信息科技有限公司"
                },
                "UpdateTime" : cst_tz.localize(datetime.datetime(year,month,day, hour, 0, 0)),#更新时间
                "StockCountAll" : 99999,#总库存数，固定
                "StockCount" : 99999,#库存数，固定
                "SallCount" : SallCount,#固定
                "AddUser" : {#添加人员，运营
                    "ForeignKeyID" : ObjectId("000000000000000000002251"),
                    "EmployeeName" : "系统管理员Online",
                    "EmployeeDepartmentID" : ObjectId("000000000000000000000781"),
                    "EmployeeDepartmentName" : "浙江恒越信息科技有限公司"
                },
                "AddTime" : cst_tz.localize(datetime.datetime(year,month,day, hour, 0, 0)),#添加时间
                "DelStatus" : 0#删除状态
            }

            stockDelivery = {
                "StockId" : item_stock["_id"],
                "ProductId" : item_stock["ProductId"],
                "TargetDate" : cst_tz.localize(datetime.datetime(year,month,day, hour, 0, 0)), #本字典有效的日期，一天一换
                "DeliverCount" : 9999, #限售次数/天
                "DeliverStatus" : 1, #固定
                "AddUser" : {
                    "ForeignKeyID": ObjectId("000000000000000000002251"),
                    "EmployeeName": "系统管理员Online",
                    "EmployeeDepartmentID": ObjectId("000000000000000000000781"),
                    "EmployeeDepartmentName": "浙江恒越信息科技有限公司"
                },
                "ChangeUser" : {

                },
                "AddTime" : cst_tz.localize(datetime.datetime(year,month,day, hour, 0, 0)), #添加时间
                "ChangeTime" : cst_tz.localize(datetime.datetime(year,month,day, hour, 0, 0)), #修改时间，暂时同AddTime
                "DelStatus" : 0
        }
        mongoConnect["MallStock"].insert(stocks)
        mongoConnect["MallStockPrice"].insert(stockPrice)
        mongoConnect["MallStockDeliverDaily"].insert(stockDelivery)
        break

# def customer():
#     cursor_order = mongoConnect["NNWOrder"].find().batch_size(30)



if __name__ == '__main__':
    # product()
    order()