'''
Created by auto_sdk on 2019.07.18
'''
from top.api.base import RestApi
class AlitripTravelVisaApplicantImportRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.form_data_json = None
		self.nation_id = None
		self.outer_apply_id = None
		self.passport_file = None
		self.passport_file_type = None
		self.photo_file = None
		self.photo_file_type = None

	def getapiname(self):
		return 'alitrip.travel.visa.applicant.import'

	def getMultipartParas(self):
		return ['photo_file','passport_file']
