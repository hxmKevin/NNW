'''
Created by auto_sdk on 2019.07.09
'''
from top.api.base import RestApi
class AlitripTravelVisaApplicantUpdateRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.applicant_infos = None
		self.applicant_op = None
		self.file_bytes = None
		self.flight_booking_form_bytes = None
		self.flight_booking_form_type = None
		self.hotel_booking_form_bytes = None
		self.hotel_booking_form_type = None
		self.oper_type = None
		self.passport_bytes = None
		self.passport_type = None
		self.photo_bytes = None
		self.photo_type = None
		self.sub_order_id = None

	def getapiname(self):
		return 'alitrip.travel.visa.applicant.update'

	def getMultipartParas(self):
		return ['flight_booking_form_bytes','photo_bytes','passport_bytes','file_bytes','hotel_booking_form_bytes']
