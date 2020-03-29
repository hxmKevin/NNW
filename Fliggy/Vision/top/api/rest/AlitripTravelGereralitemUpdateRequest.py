'''
Created by auto_sdk on 2019.08.15
'''
from top.api.base import RestApi
class AlitripTravelGereralitemUpdateRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.base_info = None
		self.booking_rules = None
		self.common_sku_list = None
		self.date_sku_info_list = None
		self.item_ele_cert_info = None
		self.item_refund_info = None
		self.poi = None

	def getapiname(self):
		return 'alitrip.travel.gereralitem.update'
