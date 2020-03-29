'''
Created by auto_sdk on 2019.01.18
'''
from top.api.base import RestApi
class AlitripTravelTradesSearchRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.category = None
		self.current_page = None
		self.end_created_time = None
		self.order_status = None
		self.page_size = None
		self.start_created_time = None

	def getapiname(self):
		return 'alitrip.travel.trades.search'
