'''
Created by auto_sdk on 2019.06.13
'''
from top.api.base import RestApi
class AlitripLocalplayProductUploadRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.confirm_time = None
		self.confirm_type = None
		self.desc_html = None
		self.desc_xml = None
		self.fee_exclude = None
		self.from_locations = None
		self.has_discount = None
		self.is_overseas_tour = None
		self.item_custom_tag = None
		self.item_id = None
		self.order_info = None
		self.out_product_id = None
		self.pic_urls = None
		self.refund_regulations = None
		self.refund_regulations_json = None
		self.refund_type = None
		self.reserve_limit = None
		self.second_kill = None
		self.seller_cids = None
		self.sub_stock = None
		self.sub_titles = None
		self.title = None
		self.to_locations = None
		self.tourist_service_provider = None
		self.traveller_template_id = None
		self.trip_day = None
		self.trip_night = None
		self.wap_desc = None

	def getapiname(self):
		return 'alitrip.localplay.product.upload'
