'''
Created by auto_sdk on 2018.07.25
'''
from top.api.base import RestApi
class AlitripTravelBaseinfoScenicsGetRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.city = None
		self.scenic = None
		self.scenic_id = None

	def getapiname(self):
		return 'taobao.alitrip.travel.baseinfo.scenics.get'
