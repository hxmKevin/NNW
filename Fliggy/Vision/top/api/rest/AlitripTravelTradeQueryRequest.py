'''
Created by auto_sdk on 2019.07.11
'''
from top.api.base import RestApi
class AlitripTravelTradeQueryRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.order_id = None

	def getapiname(self):
		return 'alitrip.travel.trade.query'
