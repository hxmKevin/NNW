'''
Created by auto_sdk on 2018.07.25
'''
from top.api.base import RestApi
class AlitripTravelNormalvisaUploadfileRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.biz_order_id = None
		self.file_bytes = None
		self.file_name = None

	def getapiname(self):
		return 'taobao.alitrip.travel.normalvisa.uploadfile'

	def getMultipartParas(self):
		return ['file_bytes']
