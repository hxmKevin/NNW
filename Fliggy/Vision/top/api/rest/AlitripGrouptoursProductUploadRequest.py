'''
Created by auto_sdk on 2018.07.25
'''
from top.api.base import RestApi
class AlitripGrouptoursProductUploadRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.back_traffic_type = None
		self.confirm_time = None
		self.confirm_type = None
		self.desc_html = None
		self.desc_xml = None
		self.electron_contract = None
		self.fee_exclude = None
		self.fee_include = None
		self.from_locations = None
		self.gather_places = None
		self.go_traffic_type = None
		self.is_overseas_tour = None
		self.item_custom_tag = None
		self.item_id = None
		self.itineraries = None
		self.order_info = None
		self.out_product_id = None
		self.pic_urls = None
		self.pure_play = None
		self.refund_regulations = None
		self.refund_type = None
		self.reserve_limit = None
		self.route_type = None
		self.struct_itineraries = None
		self.sub_stock = None
		self.sub_titles = None
		self.title = None
		self.to_locations = None
		self.traveller_template_id = None
		self.trip_day = None
		self.trip_night = None
		self.wap_desc = None

	def getapiname(self):
		return 'alitrip.grouptours.product.upload'
