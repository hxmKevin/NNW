'''
Created by auto_sdk on 2018.07.25
'''
from top.api.base import RestApi
class AlitripTravelNormalvisaUpdatepersonstautsRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.biz_order_id = None
		self.normal_visa_update_unit_list = None

	def getapiname(self):
		return 'taobao.alitrip.travel.normalvisa.updatepersonstauts'
