# coding=UTF-8
# !/usr/bin/python3
# 常量文件

class _const(object):
    class ConstError(PermissionError):pass
    def __setattr__(self, name, value):
        if name in self.__dict__.keys():
            raise self.ConstError("Can't rebind const(%s)" % name)
        self.__dict__[name]=value

    def __delattr__(self, name):
        if name in self.__dict__:
            raise  self.ConstError("Can't unbind const(%s)" % name)
        raise  NameError(name)


const = _const()

const.erp_local = 'http://erplocal.iflying.com:8899/'
const.product_local = 'http://erplocal.iflying.com:9900/'
const.erp_inner = 'http://erpinner.iflying.com:8899/'
const.product_inner = 'http://erpinner.iflying.com:9900/'
const.erp_production = 'https://erp.iflying.com/'

const.system_userid = '000000000000000000002251'

# 阿里-飞猪相关
const.appkey_main = '27761322'
const.secret_main = '98e36ac9fa56d9c5af04a6777a67d7af'