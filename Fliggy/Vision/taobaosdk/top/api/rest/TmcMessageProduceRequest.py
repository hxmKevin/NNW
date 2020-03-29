'''
Created by auto_sdk on 2018.10.23
'''
from top.api.base import RestApi
class TmcMessageProduceRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.content = None
		self.ex_content = None
		self.media_content = None
		self.media_content2 = None
		self.media_content3 = None
		self.media_content4 = None
		self.media_content5 = None
		self.target_appkey = None
		self.target_group = None
		self.topic = None

	def getapiname(self):
		return 'taobao.tmc.message.produce'

	def getMultipartParas(self):
		return ['media_content','media_content4','media_content5','media_content2','media_content3']
