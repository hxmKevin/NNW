'''
Created by auto_sdk on 2018.07.20
'''
from top.api.base import RestApi
class AlibabaGpuSchemaAddRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)
		self.brand_id = None
		self.leaf_cat_id = None
		self.provider_id = None
		self.schema_xml_fields = None

	def getapiname(self):
		return 'alibaba.gpu.schema.add'
