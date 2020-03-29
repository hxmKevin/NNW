'''
Created by auto_sdk on 2018.07.25
'''
from top.api.base import RestApi
class AlitripTravelItemBaseModifyRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.base_info = None
		self.booking_rules = None
		self.cruise_item_ext = None
		self.fields_to_clean = None
		self.freedom_item_ext = None
		self.group_item_ext = None
		self.item_id = None
		self.itineraries = None
		self.refund_info = None
		self.sales_info = None
		self.tcwl_item_ext = None

	def getapiname(self):
		return 'taobao.alitrip.travel.item.base.modify'
