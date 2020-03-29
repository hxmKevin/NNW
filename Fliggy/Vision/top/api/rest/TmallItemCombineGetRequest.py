'''
Created by auto_sdk on 2019.04.03
'''
from top.api.base import RestApi
class TmallItemCombineGetRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.item_id = None

	def getapiname(self):
		return 'tmall.item.combine.get'
