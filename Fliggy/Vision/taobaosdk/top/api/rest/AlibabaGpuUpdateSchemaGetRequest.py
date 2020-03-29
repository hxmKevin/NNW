'''
Created by auto_sdk on 2018.07.20
'''
from top.api.base import RestApi
class AlibabaGpuUpdateSchemaGetRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.product_id = None
		self.provider_id = None

	def getapiname(self):
		return 'alibaba.gpu.update.schema.get'
