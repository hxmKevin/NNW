import os
from pymongo import MongoClient

# 默认开发环境中数据源的配置

# MilestoneSOURCEMONGO = {"MilestoneSOURCEMONGOURI": "mongodb://feiyangERP:TalkIsCheap_2015@47.99.70.148:27200/erp", "MilestoneSOURCEMONGODB": "erp"}

# MilestoneSOURCEMONGO["client"] = MongoClient(MilestoneSOURCEMONGO["MilestoneSOURCEMONGOURI"])

# MilestoneSOURCEMONGO["db"] = MilestoneSOURCEMONGO["client"][MilestoneSOURCEMONGO["MilestoneSOURCEMONGODB"]]

# 默认开发环境中的配置
# MilestoneMONGO = {"MilestoneMONGOURI": "mongodb://192.168.88.4:27017", "MilestoneMONGODB": "erp"}

# 公司开发环境中的配置
MilestoneMONGO = {"MilestoneMONGOURI": "mongodb://feiyangERP:TalkIsCheap_2015@172.16.125.15:27200/erp", "MilestoneMONGODB": "erp"}

# 公司开发环境中的配置
# MilestoneMONGO = {"MilestoneMONGOURI": "mongodb://172.16.61.44:27017", "MilestoneMONGODB": "erp"}

# 家中
# MilestoneMONGO = {"MilestoneMONGOURI": "mongodb://172.168.0.101:27017", "MilestoneMONGODB": "erp"}

# 正式库
# MilestoneMONGO = {"MilestoneMONGOURI": "mongodb://feiyangERP:TalkIsCheap_2015@47.99.70.148:27200/erp", "MilestoneMONGODB": "erp"}

# 自定义环境的配置
for var in ['MilestoneMONGOURI', "MilestoneMONGODB"]:
    if os.environ.get(var):
        MilestoneMONGO[var] = os.environ.get(var)

MilestoneMONGO["client"] = MongoClient(MilestoneMONGO["MilestoneMONGOURI"])

MilestoneMONGO["db"] = MilestoneMONGO["client"][MilestoneMONGO["MilestoneMONGODB"]]
