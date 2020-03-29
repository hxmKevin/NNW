'''
Created by auto_sdk on 2019.06.03
'''
from top.api.base import RestApi
class IflyingFliggyVisaorderinfoQueryRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.orderID = None

	def getapiname(self):
		return 'wohyz4oa95.iflying.fliggy.visaorderinfo.query'
