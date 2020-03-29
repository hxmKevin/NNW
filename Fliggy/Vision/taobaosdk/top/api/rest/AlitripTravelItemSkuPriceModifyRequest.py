'''
Created by auto_sdk on 2018.07.25
'''
from top.api.base import RestApi
class AlitripTravelItemSkuPriceModifyRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.item_id = None
		self.out_product_id = None
		self.skus = None

	def getapiname(self):
		return 'taobao.alitrip.travel.item.sku.price.modify'
