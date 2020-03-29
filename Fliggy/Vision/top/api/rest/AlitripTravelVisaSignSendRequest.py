'''
Created by auto_sdk on 2019.08.02
'''
from top.api.base import RestApi
class AlitripTravelVisaSignSendRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.apply_ids = None
		self.nation_id = None
		self.sign_type = None

	def getapiname(self):
		return 'alitrip.travel.visa.sign.send'
