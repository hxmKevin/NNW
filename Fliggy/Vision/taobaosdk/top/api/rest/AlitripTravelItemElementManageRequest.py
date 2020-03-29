'''
Created by auto_sdk on 2018.07.25
'''
from top.api.base import RestApi
class AlitripTravelItemElementManageRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.city = None
		self.desc = None
		self.element_type = None
		self.name = None
		self.operation = None
		self.outer_id = None
		self.type = None

	def getapiname(self):
		return 'taobao.alitrip.travel.item.element.manage'
