import configparser
import os


class ReadConfig:
    """定义一个读取配置文件的类"""
    def __init__(self, filepath=None):
        if filepath:
            configpath = filepath
        else:
            # temp_dir = os.path.dirname(os.path.abspath('.'))
            # s1 = temp_dir.index('python_work_erp')
            # root_dir = temp_dir[0:s1] + 'python_work_erp/'
            # configpath = os.path.join(root_dir, "common/config.ini")

            temp_dir = os.path.dirname(os.path.abspath('.'))
            s1 = temp_dir.index('python_work')
            root_dir = temp_dir[0:s1] + 'python_work/'
            configpath = os.path.join(root_dir, "common/config.ini")

        self.cf = configparser.ConfigParser()
        self.cf.read(configpath)

    def get_db(self, param):
        value = self.cf.get("mongodb", param)
        return value

    def get_url(self, param):
        value = self.cf.get("url", param)
        return value

    def get_mq(self, param):
        value = self.cf.get("mq", param)
        return value

    def get_nnw_param(self, param):
        value = self.cf.get("nnw", param)
        return value


if __name__ == '__main__':
    pass
    # res = ReadConfig()
    # t = res.get_db("mongodb_ip")
    # print(t)
