'''
Created by auto_sdk on 2019.06.05
'''
from top.api.base import RestApi
class AlitripTravelVisaApplicantQueryRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.param0 = None

	def getapiname(self):
		return 'alitrip.travel.visa.applicant.query'
