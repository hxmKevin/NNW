'''
Created by auto_sdk on 2019.09.06
'''
from top.api.base import RestApi
class IflyingFliggySynrecordlistsSearchtwoRequest(RestApi):
	def __init__(self,domain='gw.api.taobao.com',port=80):
		RestApi.__init__(self,domain, port)

	def getapiname(self):
		return 'wohyz4oa95.iflying.fliggy.synrecordlists.searchtwo'
