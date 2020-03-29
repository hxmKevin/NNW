'''
Created by auto_sdk on 2018.07.25
'''
from top.api.base import RestApi
class AlitripTravelItemBaseAddRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.base_info = None
		self.booking_rules = None
		self.cruise_item_ext = None
		self.freedom_item_ext = None
		self.group_item_ext = None
		self.itineraries = None
		self.refund_info = None
		self.sales_info = None
		self.tcwl_item_ext = None

	def getapiname(self):
		return 'taobao.alitrip.travel.item.base.add'
