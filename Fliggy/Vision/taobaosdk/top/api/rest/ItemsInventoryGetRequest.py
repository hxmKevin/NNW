'''
Created by auto_sdk on 2019.04.04
'''
from top.api.base import RestApi
class ItemsInventoryGetRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.auction_type = None
		self.banner = None
		self.cid = None
		self.end_modified = None
		self.fields = None
		self.has_discount = None
		self.is_ex = None
		self.is_taobao = None
		self.order_by = None
		self.page_no = None
		self.page_size = None
		self.q = None
		self.seller_cids = None
		self.start_modified = None

	def getapiname(self):
		return 'taobao.items.inventory.get'
