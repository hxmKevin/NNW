import os

if not os.environ.get("MilestoneENV"):
    os.environ["MilestoneENV"] = "development"

if os.environ["MilestoneENV"].lower() == "development":
    # 开发环境的配置

    # Milestone数据库
    os.environ["MilestoneMONGOURI"] = "mongodb://192.168.88.4:27017"
    os.environ['MilestoneMONGODB'] = "Milestone"

elif os.environ["MilestoneENV"].lower() == "test":
    # 测试环境的配置
    pass

elif os.environ["MilestoneENV"].lower() == "production":
    # 生产环境的配置
    pass
