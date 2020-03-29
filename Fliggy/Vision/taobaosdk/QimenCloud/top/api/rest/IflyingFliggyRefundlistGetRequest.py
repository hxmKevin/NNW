'''
Created by auto_sdk on 2019.06.11
'''
from top.api.base import RestApi
class IflyingFliggyRefundlistGetRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.oid = None
		self.tid = None

	def getapiname(self):
		return 'wohyz4oa95.iflying.fliggy.refundlist.get'
