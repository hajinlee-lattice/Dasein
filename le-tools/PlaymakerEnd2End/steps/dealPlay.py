"""
@author bwang
@createDate 11/11/2015 
""" 
try:
	import pyodbc
except ImportError:
	import os
	assert os.system('pip install -U pyodbc') ==0
	import pyodbc
import time,sys,json,requests
requests.packages.urllib3.disable_warnings()
sys.path.append("..")
from Configuration.Properties import SalePrismEnvironments
log=SalePrismEnvironments.log
class DealPlay(object):
	def __init__(self):
		#got login cookie
		loginUrl="https://"+SalePrismEnvironments.host+"/"+SalePrismEnvironments.tenantName+"_Application/WebLEApplicationServiceHost.svc/Login"
		AuthorizationStr=SalePrismEnvironments.AuthorizationStr
		loginGetHeaders={'LEAuthorization':AuthorizationStr}
		response=requests.get(loginUrl,headers=loginGetHeaders,verify=False)
		assert response.status_code==200
		cookieList=response.headers.get('set-cookie').split(";")
		self.aspNet=cookieList[0]
		assert self.aspNet!=None
		self.aspAuth=cookieList[3].split("=")[1]
		assert self.aspAuth!=None
	def setPlaymakerConfigurationByRest(self,tenant=SalePrismEnvironments.tenantName,host=SalePrismEnvironments.host,useDataPlatform=SalePrismEnvironments.withModelingOnDataPlatform):
		log.info("##########  playmaker system configuration start   ##########")
		with open('..\\SysConfig') as jsonData:
			sysConfigJson=json.load(jsonData)
		conn = pyodbc.connect(DRIVER='{SQL SERVER}',SERVER=host,DATABASE=tenant,UID=SalePrismEnvironments.DBUser,PWD=SalePrismEnvironments.DBPwd)
		cur = conn.cursor()
		assert cur!=None
		keys=sysConfigJson.keys()
		try:
			for k in keys:
				value=sysConfigJson.get(k)
				if k == "AppConfig.System.DataLoaderTenantName":
					value=tenant
				if k== "AppConfig.System.EnableModelingService":
					if useDataPlatform == "FALSE":
						value="FALSE"
					else:
						value="TRUE"
				updateSQL="update ["+tenant+"].[dbo].[ConfigSystem] set Value='"+value+"'  where ["+tenant+"].[dbo].[ConfigSystem].[External_ID] = '"+k+"'"
				cur.execute(updateSQL)
				conn.commit()
		except Exception,e:
			log.error(e)
		else:
			print "Playmaker Confuguration DB  update successfully"
		finally:
			conn.close()
		#validate
		validateUrl="https://"+host+"/"+tenant+"_Application/LEApplicationServiceHost.svc?method=ValidateConfiguration&user=28"
		validateHeaders={"Cookie":self.aspNet,"Host":host,"Accept-Encoding":"gzip, deflate","SOAPAction":"http://tempuri.org/ILEApplicationService/ValidateConfiguration","LEFormsTicket":self.aspAuth,"Content-Type":"text/xml; charset=utf-8","User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36","Origin":"https://"+host}
		validateXML=SalePrismEnvironments.validatePostXML
		response=requests.post(validateUrl,data=validateXML,headers=validateHeaders,verify=False)
		assert response.status_code==200
		print "waiting for validation process"
		time.sleep(88)
		#resetCache
		resetCacheUrl="https://"+host+"/"+tenant+"_Application/LEApplicationServiceHost.svc?method=ResetCache&user=28"
		resetCacheHeaders={"Cookie":self.aspNet,"Host":host,"Accept-Encoding":"gzip, deflate","SOAPAction":"http://tempuri.org/ILEApplicationService/ResetCache","LEFormsTicket":self.aspAuth,"Content-Type":"text/xml; charset=utf-8","User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36","Origin":"https://"+host}
		resetCacheXML=SalePrismEnvironments.resetCachePostXML
		response=requests.post(resetCacheUrl,data=resetCacheXML,headers=resetCacheHeaders,verify=False)
		assert response.status_code==200
		log.info("reset cache successfully")

	def createPlayByREST(self,playType=SalePrismEnvironments.playType,playName=SalePrismEnvironments.playName):
		#deal post data
		log.info("##########  play creation starts   ##########")
		with open("..\\PlaysCreationJsonFiles\\"+playType) as createPlayJsonFile:
			createPlayJson=json.load(createPlayJsonFile)
		createPlayJson['DisplayName']=playName
		createPlayJson['ExternalID']=createPlayJson['DisplayName']+"_"+str(int(time.time()))
		#post create play data
		savePlayUrl=SalePrismEnvironments.savePlayUrl
		createPlayHeaders={"Cookie":self.aspNet,"Host":SalePrismEnvironments.host,"Accept":"application/json, text/javascript, */*; q=0.01","LEFormsTicket":self.aspAuth,"Content-Type":"application/json; charset=UTF-8","User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36","Origin":"https://"+SalePrismEnvironments.host}
		response=requests.post(savePlayUrl,json=createPlayJson,headers=createPlayHeaders,verify=False)
		assert response.status_code==200
		resJson=json.loads(str(response.text))
		assert resJson['Success']==True
		PlayID=json.loads(resJson["CompressedResult"])["Key"]
		assert int(PlayID)>0
		log.info("Play created!")
		return PlayID
	def scorePlay(self,idOfPlay):
		log.info("##########  Play Scoring start   ##########")
		scorePlayUrl=SalePrismEnvironments.scorePlayUrl+str(idOfPlay)
		scorePlayHeaders={"Cookie":self.aspNet,"Host":SalePrismEnvironments.host,"Accept":"*/*; q=0.01","LEFormsTicket":self.aspAuth,"User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36","Origin":"https://"+SalePrismEnvironments.host}
		response=requests.post(scorePlayUrl,headers=scorePlayHeaders,verify=False)
		assert response.status_code==200
	def approvePlay(self,idOfPlay):
		approvePlayUrl=SalePrismEnvironments.approvePlayUrl.replace("99",str(idOfPlay))
		approvePlayHeaders={"Cookie":self.aspNet,"Host":SalePrismEnvironments.host,"Accept":"*/*; q=0.01","LEFormsTicket":self.aspAuth,"User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36","Origin":"https://"+SalePrismEnvironments.host}
		response=requests.post(approvePlayUrl,headers=approvePlayHeaders,verify=False)
		assert response.status_code==200
		log.info("##########  play approved! ready to launch   ##########")
	def getStatusOfPlay(self,idOfPlay):
		getStatusOfPlayUrl=SalePrismEnvironments.getStatusOfPlayUrl+str(idOfPlay)
		getStatusOfPlayHeaders={"Cookie":self.aspNet,"Host":SalePrismEnvironments.host,"Accept":"*/*; q=0.01","LEFormsTicket":self.aspAuth}
		response=requests.get(getStatusOfPlayUrl,headers=getStatusOfPlayHeaders,verify=False)
		assert response.status_code==200
		resultJson=json.loads(response.text)
		status=json.loads(resultJson['CompressedResult'])['CombinedModelScoreStatusID']
		return status
	def launchPlay(self,nameOfPlayToLaunch=SalePrismEnvironments.playName,launchAllPlays=False):
		log.info("##########  Play Launch starts   ##########")
		getPortfililPlaysUrl=SalePrismEnvironments.getPortfililPlaysUrl
		getPortfililPlaysHeaders={"Cookie":self.aspNet,"Host":SalePrismEnvironments.host,"LEFormsTicket":self.aspAuth,"Origin":"https://"+SalePrismEnvironments.host}
		response=requests.get(getPortfililPlaysUrl,headers=getPortfililPlaysHeaders,verify=False)
		assert response.status_code==200
		resJson=json.loads(response.text)
		compressedResult=resJson["CompressedResult"]
		compressedResult=compressedResult[1:-1]
		playLaunchJsonList=compressedResult.split("},{")
		assert playLaunchJsonList!=None
		#launch play
		launchPlaysUrl=SalePrismEnvironments.launchPlaysUrl
		launchPlaysHeaders={"Cookie":self.aspNet,"Host":SalePrismEnvironments.host,"Accept":"application/json, text/javascript, */*; q=0.01","LEFormsTicket":self.aspAuth,"Content-Type":"application/json; charset=UTF-8","User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36","Origin":"https://"+SalePrismEnvironments.host,"Referer":"https://bodcdevvqap25.dev.lattice.local/20151113142130283_Application/salesprism.aspx"}
		for jsonString in playLaunchJsonList:
			if not jsonString.startswith("{"):
				jsonString="{"+jsonString
			if not jsonString.endswith("}"):
				jsonString=jsonString+"}"
			if launchAllPlays:
				realJson=json.loads(jsonString)
				realJson["LaunchRuleDisplayName"]="Create CRM recommendations"
				realJson["LaunchRuleExternalID"]="LaunchAsSFDCSync"
				realJson["LaunchPlayFlag"]=True
				realJson["SettingsPopUpVisible"]=False
				realJson["LaunchRuleAvailability"]="PlaysWithNewLeads"
				realJson=json.loads("["+str(realJson)+"]")
				response=requests.post(launchPlaysUrl,json=realJson,headers=launchPlaysHeaders,verify=False)
				assert response.status_code==200
				log.info("play %s launched successfully"%jsonString)
			elif jsonString.find(nameOfPlayToLaunch) > 0:
				realJson=json.loads(jsonString)
				realJson["LaunchRuleDisplayName"]="Create CRM recommendations"
				realJson["LaunchRuleExternalID"]="LaunchAsSFDCSync"
				realJson["LaunchPlayFlag"]=True
				realJson["SettingsPopUpVisible"]=False
				realJson["LaunchRuleAvailability"]="PlaysWithNewLeads"
				realJson=json.loads("["+json.dumps(realJson)+"]")
				response=requests.post(launchPlaysUrl,json=realJson,headers=launchPlaysHeaders,verify=False)
				assert response.status_code==200
				log.info("play %s launched successfully"%nameOfPlayToLaunch)
				break
	def getLaunchStatus(self,idOfPlay):
		getLaunchStatusUrl=SalePrismEnvironments.getLaunchStatusUrl+str(idOfPlay)
		getLaunchStatusHeaders={"Cookie":self.aspNet,"Host":SalePrismEnvironments.host,"LEFormsTicket":self.aspAuth,"Origin":"https://"+SalePrismEnvironments.host}
		response=requests.get(getLaunchStatusUrl,headers=getLaunchStatusHeaders,verify=False)
		temp=json.loads(json.loads(response.text)['CompressedResult'])['Value']
		assert temp!=None
		return temp["LaunchStatus"]
if __name__=='__main__':
	Play=DealPlay()
	PlayID=Play.createPlayByREST()
	Play.approvePlay(idOfPlay=PlayID)
	Play.scorePlay(idOfPlay=PlayID)
	status=Play.getStatusOfPlay(idOfPlay=PlayID)
	while status != 'Complete':
		time.sleep(10)
		status=Play.getStatusOfPlay(idOfPlay=PlayID)
	print Play.getLaunchStatus(idOfPlay=PlayID)
	Play.launchPlay()
	Play.getLaunchStatus(idOfPlay=PlayID)
