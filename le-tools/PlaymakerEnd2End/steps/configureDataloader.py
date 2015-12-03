"""
@author bwang
@createDate 11/11/2015 
""" 
try:
	import requests
except Exception,e:
	import os
	assert os.system('pip install requests') ==0
	import requests
requests.packages.urllib3.disable_warnings()
import json,sys,time
sys.path.append("..")
from Configuration.Properties import SalePrismEnvironments
log=SalePrismEnvironments.log
class DataloaderDealer(object):
	def __init__(self):
		self.headers={"MagicAuthentication":"Security through obscurity!","Accept":"application/json;","Content-Type":"application/json; charset=utf-8;"}
	def setTenantDataProviderByREST(self,tenant=SalePrismEnvironments.tenantName,host=SalePrismEnvironments.host,dbUser=SalePrismEnvironments.DBUser,dbPwd=SalePrismEnvironments.DBPwd):
		log.info("##########  dataloader configuration start   ##########")
		RESTurl=SalePrismEnvironments.dataloaderUpdateRESTURL
		AnalyticsDBJson={"tenantName": tenant,"dataProviderName": "AnalyticsDB","dataSourceType": "SQL","values": [{"Key": "ServerName","Value": host},{"Key": "Authentication","Value": "SQL Server Authentication"},{"Key": "User","Value": dbUser},{"Key": "Password","Value": dbPwd},{"Key": "Database","Value": tenant},{"Key": "Schema","Value": "dbo"}]}
		DanteDBJson={"tenantName": tenant,"dataProviderName": "DanteDB","dataSourceType": "SQL","values": [{"Key": "ServerName","Value": host},{"Key": "Authentication","Value": "SQL Server Authentication"},{"Key": "User","Value": dbUser},{"Key": "Password","Value": dbPwd},{"Key": "Database","Value": "DT_"+tenant},{"Key": "Schema","Value": "dbo"}]}
		PlayMakerDBJson={"tenantName": tenant,"dataProviderName": "PlayMakerDB","dataSourceType": "SQL","values": [{"Key": "ServerName","Value": host+"\\SQL2012STD"},{"Key": "Authentication","Value": "SQL Server Authentication"},{"Key": "User","Value": dbUser},{"Key": "Password","Value": dbPwd},{"Key": "Database","Value": tenant},{"Key": "Schema","Value": "dbo"}]}
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
	def isDanteGroupFinishSuccessfully(self,tenant=SalePrismEnvironments.tenantName):
		log.info("FULL_DANTE_DATA_FLOW is running, this may cost lof of time, please wait")
		ff=SalePrismEnvironments.driver2
		ff.get(SalePrismEnvironments.dataloaderUrl)
		emailInput=ff.find_element_by_id('text_email_login')
		emailInput.clear()
		emailInput.send_keys('bwang@lattice-engines.com')
		#input password
		pwdInput=ff.find_element_by_id('text_password_login')
		pwdInput.clear()
		pwdInput.send_keys('1')
		#click login
		ff.find_element_by_xpath("//input[@value='Sign In']").click()
		time.sleep(2)#in case of element not found error
		#change tenant to specified one
		ff.find_element_by_xpath("//li[@id='li_account']").click()
		ff.find_element_by_xpath("//li[@id='li_changetenant']").click()
		time.sleep(3)
		ff.find_element_by_xpath("//span[starts-with(text(),'"+tenant+"')]").click()
		ff.find_element_by_xpath("//span[text()='OK']").click()
		time.sleep(3)
		launchId=ff.find_element_by_xpath("//div[@id='div_queue_launches']//td[2]").text
		assert launchId!=None
		log.info("The running FULL_DANTE_DATA_FLOW ID is %s "%launchId)
		ff.quit()
		RESTurl=SalePrismEnvironments.dataloaderGetLaunchStatusURL
		fullDanteDataFlowJson={"launchId":int(launchId)}
		isDanteGroupFinishSuccessfully=False
		stillRunning="True"
		while stillRunning=="True":
			time.sleep(60)
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
					else:
						log.error(responseValue[4]["Value"])
					log.info(responseValue[4]["Value"])
					break
			except Exception,e:
				log.error(e)
		return isDanteGroupFinishSuccessfully


def main():
	d=DataloaderDealer()
	print d.isDanteGroupFinishSuccessfully()
if __name__ == '__main__':
	main()