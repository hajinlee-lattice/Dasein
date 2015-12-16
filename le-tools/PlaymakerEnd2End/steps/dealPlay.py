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
import time,json,requests,csv,os
#from poster.encode import multipart_encode
#from poster.streaminghttp import register_openers
import urllib2,ssl

ssl._create_default_https_context=ssl._create_unverified_context
requests.packages.urllib3.disable_warnings()
from PlaymakerEnd2End.Configuration.Properties import SalePrismEnvironments
from PlaymakerEnd2End.tools.DBHelper import DealDB
log=SalePrismEnvironments.log
class PlayTypes(object):
	t_allTypes=['CSRepeatPurchase','CSFirstPurchase','LatticeGenerates','RuleBased','Winback','List','RenewalList','AnalyticList']
	t_listTypes=['List','RenewalList','AnalyticList']
	t_otherTypes=['CSRepeatPurchase','CSFirstPurchase','LatticeGenerates','RuleBased','Winback']
	t_AnalyticList='AnalyticList'
	t_CSRepeatPurchase='CSRepeatPurchase'
	t_CSFirstPurchase='CSFirstPurchase'
	t_LatticeGenerates='LatticeGenerates'
	t_List='List'
	t_RuleBased='RuleBased'
	t_Winback='Winback'
	t_RenewalList="RenewalList"
class StatusCode(object):
	s_Created=1000
	s_Launched=2800
	s_Synced=3000#need to investigate

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
		self.postFileUrl=SalePrismEnvironments.SPUploadFileUrl+"?LEFormsTicket="+self.aspAuth+"&UploadManager=PlayAccountList&UploadAlgorithm=UploadAccountList&PlayID=99&SourceFileName=WestPhysicsWithProbability.csv"
	def setPlaymakerConfigurationByRest(self,tenant=SalePrismEnvironments.tenantName,host=SalePrismEnvironments.host,useDataPlatform=SalePrismEnvironments.withModelingOnDataPlatform):
		log.info("##########  playmaker system configuration start   ##########")
		with open(SalePrismEnvironments.rootPath+'PlayMakerSysConfig.json') as jsonData:
			sysConfigJson=json.load(jsonData)
		conn = pyodbc.connect(DRIVER=SalePrismEnvironments.ODBCSqlServer,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=tenant,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword)
		cur = conn.cursor()
		assert cur!=None
		keys=sysConfigJson.keys()
		try:
			for k in keys:
				value=sysConfigJson.get(k)
				if k == "AppConfig.System.DataLoaderTenantName":
					value=tenant
				if k== "AppConfig.System.EnableModelingService":
					if str(useDataPlatform).upper() == "FALSE":
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
	def createPlayByREST(self,UseEVModel=False,playType=SalePrismEnvironments.playType):
		if SalePrismEnvironments.needCleanUpTenantDB:
			self.cleanUpPlaysAndPreleads()
		log.info("##########  play creation starts   ##########")
		playName=playType+repr(time.time()).replace('.','')
		time.sleep(1)
		playExternalId=playName+"_"+repr(time.time()).replace('.','')
		log.info("The play type is: %s" % (playType))
		#Prepare the list for anlytics play type
		AnlyticPlayList=[PlayTypes.t_CSFirstPurchase,PlayTypes.t_AnalyticList,PlayTypes.t_CSRepeatPurchase,PlayTypes.t_Winback]
		with open(SalePrismEnvironments.rootPath+"PlaysCreationJsonFiles\\"+playType+".json") as createPlayJsonFile:
			createPlayJson=json.load(createPlayJsonFile)
		createPlayJson['DisplayName']=playName
		createPlayJson['ExternalID']=playExternalId
		talkingPointsPrefix="TP-"+playName+"_"
		talkingPointsList=createPlayJson["TalkingPoints"]
		if len(talkingPointsList)>0:
			log.info("Modifying Talking Points timestamp")
			for index in range(0,len(talkingPointsList)):
				createPlayJson["TalkingPoints"][index]["ExternalID"]=talkingPointsPrefix+repr(time.time()).replace('.','')
				createPlayJson["TalkingPoints"][index]["PlayExternalID"]=playExternalId
				time.sleep(1)
		else:
			log.info("No Talking Points need to modify")
		if playType in AnlyticPlayList:
			log.info("This play is Anlytic play")
			if UseEVModel:
				log.info("This play has checked Use EV modeling")
				createPlayJson=self.enableEVModelInJson(createPlayJson)
			else:
				log.info("This play has unchecked Use EV modeling")
				createPlayJson=self.disableEVModelInJson(createPlayJson)
		#post create play data
		savePlayUrl=SalePrismEnvironments.savePlayUrl
		createPlayHeaders={"Cookie":self.aspNet,"Host":SalePrismEnvironments.host,"Accept":"application/json, text/javascript, */*; q=0.01","LEFormsTicket":self.aspAuth,"Content-Type":"application/json; charset=UTF-8","User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36","Origin":"https://"+SalePrismEnvironments.host}
		response=requests.post(savePlayUrl,json=createPlayJson,headers=createPlayHeaders,verify=False)
		assert response.status_code==200
		resJson=json.loads(str(response.text))
		assert resJson['Success']==True
		PlayID=json.loads(resJson["CompressedResult"])["Key"]
		assert int(PlayID)>0
		IsUnavailable=True
		while IsUnavailable==True:
			IsUnavailable=self.getStatusOfPlay(idOfPlay=PlayID,key="IsUnavailable")
		log.info("Play created!")
		log.info("Name of created Play is  "+playName)
		playDict={"playName":playName,"playId":PlayID}
		if playType in PlayTypes.t_listTypes:
			log.info("Now Uploading FIle to Server")
			self.uploadFile(idOfPlay=PlayID,playType=playType)
		return playDict
	def scorePlay(self,idOfPlay):
		log.info("##########  Play Scoring start   ##########")
		scorePlayUrl=SalePrismEnvironments.scorePlayUrl+str(idOfPlay)
		scorePlayHeaders={"Cookie":self.aspNet,"Host":SalePrismEnvironments.host,"Accept":"*/*; q=0.01","LEFormsTicket":self.aspAuth,"User-Agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.85 Safari/537.36","Origin":"https://"+SalePrismEnvironments.host}
		response=requests.post(scorePlayUrl,headers=scorePlayHeaders,verify=False)
		assert response.status_code==200

	def approvePlay(self,idOfPlay):
		approvePlayUrl=SalePrismEnvironments.approvePlayUrl.replace("99",str(idOfPlay))
		approvePlayHeaders={"Cookie":self.aspNet,"Host":SalePrismEnvironments.host,"Content-Type":"application/json","LEFormsTicket":self.aspAuth}
		response=requests.post(approvePlayUrl,headers=approvePlayHeaders,verify=False)
		resJson=json.loads(response.text)
		print type(resJson['Success'])
		assert resJson['Success']==True
		assert response.status_code==200
		log.info("##########  play approved! ready to launch   ##########")
	def getStatusOfPlay(self,idOfPlay,key='CombinedModelScoreStatusID'):
		getStatusOfPlayUrl=SalePrismEnvironments.getStatusOfPlayUrl+str(idOfPlay)
		getStatusOfPlayHeaders={"Cookie":self.aspNet,"Host":SalePrismEnvironments.host,"Accept":"*/*; q=0.01","LEFormsTicket":self.aspAuth}
		response=requests.get(getStatusOfPlayUrl,headers=getStatusOfPlayHeaders,verify=False)
		assert response.status_code==200
		resultJson=json.loads(response.text)
		status=json.loads(resultJson['CompressedResult'])[key]
		return status
	def launchPlay(self,nameOfPlayToLaunch=None,launchAllPlays=False):
		if not launchAllPlays:
			assert nameOfPlayToLaunch
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
		postJsonList=''
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
				if postJsonList=='':
					postJsonList=json.dumps(realJson)
				else:
					postJsonList=postJsonList+','+json.dumps(realJson)
			elif jsonString.find(nameOfPlayToLaunch) > 0:
				realJson=json.loads(jsonString)
				realJson["LaunchRuleDisplayName"]="Create CRM recommendations"
				realJson["LaunchRuleExternalID"]="LaunchAsSFDCSync"
				realJson["LaunchPlayFlag"]=True
				realJson["SettingsPopUpVisible"]=False
				realJson["LaunchRuleAvailability"]="PlaysWithNewLeads"
				postJsonList=json.dumps(realJson)
		time_start_launch=time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
		str_post=json.loads("["+postJsonList+"]")
		#print str_post
		response=requests.post(launchPlaysUrl,json=str_post,headers=launchPlaysHeaders,verify=False)
		assert response.status_code==200
		log.info("play %s launched successfully"%nameOfPlayToLaunch)
		return time_start_launch+'.000'

	def getLaunchStatus(self,idOfPlay):
		getLaunchStatusUrl=SalePrismEnvironments.getLaunchStatusUrl+str(idOfPlay)
		getLaunchStatusHeaders={"Cookie":self.aspNet,"Host":SalePrismEnvironments.host,"LEFormsTicket":self.aspAuth,"Origin":"https://"+SalePrismEnvironments.host}
		response=requests.get(getLaunchStatusUrl,headers=getLaunchStatusHeaders,verify=False)
		temp=json.loads(json.loads(response.text)['CompressedResult'])['Value']
		assert temp!=None
		return temp["LaunchStatus"]

	def enableEVModelInJson(self,jsonPost):
		json_EVModel=dict(jsonPost)
		json_EVModel['ModelingMethodID']='ExpectedValue'
		json_EVModel['ScoringMethodID']='ExpectedRevenue'
		log.info("Have enable EVModeling,ModelingMethodID value is %s, ScoringMethodID value is %s" % (json_EVModel['ModelingMethodID'],json_EVModel['ScoringMethodID']))
		return json_EVModel

	def disableEVModelInJson(self,jsonPost):
		json_EVModel=dict(jsonPost)
		json_EVModel['ModelingMethodID']='Standard'
		json_EVModel['ScoringMethodID']='Lift'
		log.info("Have disable EVModeling,ModelingMethodID value is %s, ScoringMethodID value is %s" % (json_EVModel['ModelingMethodID'],json_EVModel['ScoringMethodID']))
		return json_EVModel

	def cleanUpPlaysAndPreleads(self,Server=SalePrismEnvironments.tenantDBUrl,Database=SalePrismEnvironments.tenantName,User=SalePrismEnvironments.tenantDBUser,Password=SalePrismEnvironments.tenantDBPassword,Driver=SalePrismEnvironments.ODBCSqlServer):
		#set all previous plays isActive to 0
		conn=None
		try:
			conn = pyodbc.connect(DRIVER=Driver,SERVER=Server,DATABASE=Database,UID=User,PWD=Password)
			updateSql="update Play set IsActive=0"
			cur=conn.cursor()
			assert cur != None
			cur.execute(updateSql)
			conn.commit()
			#delete from Prelead
			deleteRecommendationsSql="""
				delete from RecommendationAccount
				delete from Prelead
			"""
			cur.execute(deleteRecommendationsSql)
			conn.commit()
		except Exception,e:
			log.error(e)
		finally:
			conn.close()

	def uploadFile(self,idOfPlay,playType,fileName=SalePrismEnvironments.SPUploadFileName):
		realUrl=self.postFileUrl
		realUrl=realUrl.replace('99',str(idOfPlay))
		if playType==PlayTypes.t_AnalyticList:
			realUrl=realUrl.replace('UploadAccountAndProbabilityList','UploadAccountList')
		realUrl=realUrl.replace("WestPhysicsWithProbability.csv",fileName)
		length = os.path.getsize('.\\PlaymakerEnd2End\\'+fileName)
		png_data = open('.\\PlaymakerEnd2End\\'+fileName, "rb")
		request = urllib2.Request(realUrl, data=png_data)
		request.add_header('Cookie', "_at_id.latticeengines.dantedevelopment.d1f3=7d10091f88e1cbaa.1448275356.4.1449471738.1448420905.50.108; _at_id.latticeengines.development.d1f3=57d9e67a4300bf40.1448254061.51.1449827799.1449822297.2374.39292; "+self.aspNet)
		request.add_header('Content-Length', '%d' % length)
		request.add_header('Content-Type', 'application/octet-stream')
		request.add_header('Host', 'SalePrismEnvironments.host')
		request.add_header('LEFormsTicket',self.aspAuth)
		request.add_header('X-File-Name', fileName)
		request.add_header('X-Mime-Type', fileName)
		request.add_header('X-Requested-With', 'XMLHttpRequest')
		request.add_header('Accept', '*/*')
		request.add_header('Accept-Encoding', 'gzip, deflate')
		res = urllib2.urlopen(request).read()
		dir(res)


if __name__=='__main__':
	Play=DealPlay()
	#playDict=Play.createPlayByREST()
	#PlayID=playDict.get("playId")
	#Play.approvePlay(idOfPlay=PlayID)
	#Play.scorePlay(idOfPlay=PlayID)
	#status=Play.getStatusOfPlay(idOfPlay=PlayID)
	#while status != 'Complete':
		#time.sleep(10)
		#status=Play.getStatusOfPlay(idOfPlay=PlayID)
	#print Play.getLaunchStatus(idOfPlay=PlayID)
	#Play.launchPlay()
	#Play.getLaunchStatus(idOfPlay=PlayID)
	#Play.cleanUpPlaysAndPreleads()
