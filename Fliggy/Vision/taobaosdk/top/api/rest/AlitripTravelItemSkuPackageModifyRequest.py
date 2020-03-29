'''
Created by auto_sdk on 2019.08.03
'''
from top.api.base import RestApi
class AlitripTravelItemSkuPackageModifyRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.item_id = None
		self.out_product_id = None
		self.skus = None

	def getapiname(self):
		return 'taobao.alitrip.travel.item.sku.package.modify'
