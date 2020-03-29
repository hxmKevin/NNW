'''
Created by auto_sdk on 2018.08.13
'''
from top.api.base import RestApi
class TmcTopicGroupAddRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.group_name = None
		self.topics = None

	def getapiname(self):
		return 'taobao.tmc.topic.group.add'
