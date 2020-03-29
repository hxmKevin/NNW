'''
Created by auto_sdk on 2019.08.20
'''
from top.api.base import RestApi
class AlitripTravelItemShelveRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.item_id = None
		self.item_status = None
		self.online_time = None
		self.out_product_id = None

	def getapiname(self):
		return 'taobao.alitrip.travel.item.shelve'
