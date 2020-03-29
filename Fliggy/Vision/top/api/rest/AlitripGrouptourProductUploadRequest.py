'''
Created by auto_sdk on 2019.06.13
'''
from top.api.base import RestApi
class AlitripGrouptourProductUploadRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.back_traffic_type = None
		self.confirm_time = None
		self.confirm_type = None
		self.desc_html = None
		self.desc_xml = None
		self.from_locations = None
		self.go_traffic_type = None
		self.group_tour_package_info = None
		self.group_tour_type = None
		self.has_discount = None
		self.is_overseas_tour = None
		self.item_custom_tag = None
		self.item_id = None
		self.out_product_id = None
		self.package_operation = None
		self.pic_urls = None
		self.pure_play = None
		self.refund_regulations = None
		self.refund_regulations_json = None
		self.refund_type = None
		self.reserve_limit = None
		self.route_type = None
		self.second_kill = None
		self.seller_cids = None
		self.sub_stock = None
		self.sub_titles = None
		self.title = None
		self.to_locations = None
		self.traveller_template_id = None
		self.trip_day = None
		self.trip_night = None
		self.wap_desc = None

	def getapiname(self):
		return 'alitrip.grouptour.product.upload'
