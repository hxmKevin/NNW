'''
Created by auto_sdk on 2019.09.11
'''
from top.api.base import RestApi
class IflyingShixianListGetRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.RequestData = None

	def getapiname(self):
		return '80yrm8bm30.iflying.shixian.list.get'
