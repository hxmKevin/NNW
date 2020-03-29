'''
Created by auto_sdk on 2018.10.23
'''
from top.api.base import RestApi
class TmcMessagesConsumeRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.group_name = None
		self.quantity = None

	def getapiname(self):
		return 'taobao.tmc.messages.consume'
