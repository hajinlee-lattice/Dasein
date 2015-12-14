"""
@author bwang
@createDate 11/11/2015 
"""
import json
import time

import requests

from PlaymakerEnd2End.Configuration.Properties import SalePrismEnvironments
from PlaymakerEnd2End.tools.DBHelper import DealDB

try:
	import pyodbc
except ImportError:
	import os
	os.system('pip install -U pyodbc')
requests.packages.urllib3.disable_warnings()
log=SalePrismEnvironments.log
class DataloaderDealer(object):
	def __init__(self):
		self.headers={"MagicAuthentication":"Security through obscurity!","Accept":"application/json;","Content-Type":"application/json; charset=utf-8;"}
	def setTenantDataProviderByREST(self,tenant=SalePrismEnvironments.tenantName):
		log.info("##########  dataloader configuration start   ##########")
		RESTurl=SalePrismEnvironments.dataloaderUpdateRESTURL
		AnalyticsDBJson={"tenantName": tenant,"dataProviderName": "AnalyticsDB","dataSourceType": "SQL","values": [{"Key": "ServerName","Value": SalePrismEnvironments.tenantDBUrl},{"Key": "Authentication","Value": "SQL Server Authentication"},{"Key": "User","Value": SalePrismEnvironments.tenantDBUser},{"Key": "Password","Value": SalePrismEnvironments.tenantDBPassword},{"Key": "Database","Value": tenant},{"Key": "Schema","Value": "dbo"}]}
		DanteDBJson={"tenantName": tenant,"dataProviderName": "DanteDB","dataSourceType": "SQL","values": [{"Key": "ServerName","Value": SalePrismEnvironments.tenantDBUrl},{"Key": "Authentication","Value": "SQL Server Authentication"},{"Key": "User","Value": SalePrismEnvironments.tenantDBUser},{"Key": "Password","Value": SalePrismEnvironments.tenantDBPassword},{"Key": "Database","Value": "DT_"+tenant},{"Key": "Schema","Value": "dbo"}]}
		PlayMakerDBJson={"tenantName": tenant,"dataProviderName": "PlayMakerDB","dataSourceType": "SQL","values": [{"Key": "ServerName","Value": SalePrismEnvironments.tenantDBUrl},{"Key": "Authentication","Value": "SQL Server Authentication"},{"Key": "User","Value": SalePrismEnvironments.tenantDBUser},{"Key": "Password","Value": SalePrismEnvironments.tenantDBPassword},{"Key": "Database","Value": tenant},{"Key": "Schema","Value": "dbo"}]}
		request=None
		try:
			request = requests.post(RESTurl,json=AnalyticsDBJson,headers=self.headers)
			assert request.status_code == 200
			response = json.loads(request.text)
			assert response['Success'] == True
			request = requests.post(RESTurl,json=DanteDBJson,headers=self.headers)
			assert request.status_code == 200
			response = json.loads(request.text)
			assert response['Success'] == True
			request = requests.post(RESTurl,json=PlayMakerDBJson,headers=self.headers)
			assert request.status_code == 200
			response = json.loads(request.text)
			assert response['Success'] == True
		except Exception,e:
			log.error(e)
		else:
			log.info("dataloader configuration finish successfully")
		finally:
			if request !=None:
				request.close()
	def isDanteGroupFinishSuccessfully(self,tenant=SalePrismEnvironments.tenantName,timePoint=None):
		assert timePoint!=None
		sql="SELECT LaunchId,CreateTime FROM Launches where tenantid=(SELECT TenantId FROM Tenant where name='"+tenant+"'and status=1) and GroupName='Full_Dante_Data_Flow' and createtime>'"+timePoint+"'"
		log.info('sql to query Launch ID is: '+sql)
		result=DealDB.fetchResultOfSelect(SQL=sql,SERVER=SalePrismEnvironments.dataloaderDBUrl,DATABASE=SalePrismEnvironments.dataloaderDBName,UID=SalePrismEnvironments.dataloaderDBUser,PWD=SalePrismEnvironments.dataloaderDBPassword,fetchAll=False)
		assert result!=None
		launchId=result[0]
		assert launchId !=None
		log.info("The running FULL_DANTE_DATA_FLOW ID is %s "%launchId)
		RESTurl=SalePrismEnvironments.dataloaderGetLaunchStatusURL
		fullDanteDataFlowJson={"launchId":int(launchId)}
		isDanteGroupFinishSuccessfully=False
		stillRunning="True"
		while stillRunning=="True":

			log.info("waiting for load group")
			try:
				request = requests.post(RESTurl,json=fullDanteDataFlowJson,headers=self.headers)
				assert request.status_code == 200
				responseValue = json.loads(request.text)["Value"]
				stillRunning=responseValue[2]["Value"]
				runSucceed=responseValue[3]["Value"]
				if stillRunning == "False":
					if runSucceed == "True":
						isDanteGroupFinishSuccessfully=True
						log.info(responseValue[4]["Value"])
					else:
						log.error(responseValue[4]["Value"])
					break
			except Exception,e:
				log.error(e)
			time.sleep(60)
		return isDanteGroupFinishSuccessfully


def main():
	d=DataloaderDealer()
	print d.isDanteGroupFinishSuccessfully(timePoint="2015-12-07 08:00:00")
if __name__ == '__main__':

	main()