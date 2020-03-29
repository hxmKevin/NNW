'''
Created by auto_sdk on 2018.07.25
'''
from top.api.base import RestApi
class AlitripTravelTradeCloseRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.close_reason = None
		self.reason_desc = None
		self.sub_order_id = None

	def getapiname(self):
		return 'alitrip.travel.trade.close'
