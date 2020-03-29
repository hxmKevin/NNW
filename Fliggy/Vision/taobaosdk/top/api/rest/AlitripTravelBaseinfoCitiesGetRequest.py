'''
Created by auto_sdk on 2018.07.25
'''
from top.api.base import RestApi
class AlitripTravelBaseinfoCitiesGetRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.cat_type = None
		self.ioc_type = None

	def getapiname(self):
		return 'taobao.alitrip.travel.baseinfo.cities.get'
