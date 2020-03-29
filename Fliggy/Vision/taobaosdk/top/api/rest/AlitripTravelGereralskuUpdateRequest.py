'''
Created by auto_sdk on 2018.10.22
'''
from top.api.base import RestApi
class AlitripTravelGereralskuUpdateRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.alias = None
		self.date_list = None
		self.item_id = None
		self.outer_id = None
		self.price = None
		self.properties = None
		self.quantity = None

	def getapiname(self):
		return 'alitrip.travel.gereralsku.update'
