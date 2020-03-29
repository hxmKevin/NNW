import datetime

from numpy.core.defchararray import zfill

from common.handle_mongodb import HandleMongoDB
m = HandleMongoDB()
mongoConnect = m.mongodb_connect()
from pytz import timezone

cst_tz = timezone('Asia/Shanghai')#Asia/Shanghai
year = datetime.datetime.now().year
month = datetime.datetime.now().month
day = datetime.datetime.now().day
TodayISODate = cst_tz.localize(datetime.datetime(year, month, day, 0, 0, 0))
# cursor = mongoConnect["NNWOrder"].find({"UpdateTime":{"$gte":TodayISODate}}).count()
# # print(cursor)
today = datetime.date.today()
tomorrow = today + datetime.timedelta(days=1)
# print(type(tomorrow.day))
# n = 24
# n = str(n).zfill(9)
# print(n)

print(p)
# new_product = False
# for item in cursor:
#     new_product = True
#     break
# print(new_product)
