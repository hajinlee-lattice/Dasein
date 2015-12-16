"""
@author bwang
@createDate 11/11/2015
"""
import os
import unittest
import time

from PlaymakerEnd2End.Configuration.Properties import SalePrismEnvironments
from PlaymakerEnd2End.steps.configureDataloader import DataloaderDealer
from PlaymakerEnd2End.steps.configureSFDC  import DealSFDC
from PlaymakerEnd2End.steps.dealPlay  import DealPlay
from PlaymakerEnd2End.tools.DBHelper import DealDB
from PlaymakerEnd2End.steps.dealPlay import PlayTypes
from PlaymakerEnd2End.Configuration.SetupPLME2EEnviroment import setUpEnvironments
log=SalePrismEnvironments.log

class TestSteps(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		print 'this is set up method!'
		#print SalePrismEnvironments.needSetupEnvironment
		if SalePrismEnvironments.needSetupEnvironment:
			try:
				print 'set up started'
				setUpEnvironments.setUp()
				SalePrismEnvironments.needSetupEnvironment=False
			except Exception,e:
				log.error('set up failed: '+str(e.message))
				SalePrismEnvironments.needSetupEnvironment=True
	def DataFlowForOnePlay(self,playType=PlayTypes.t_CSRepeatPurchase):
		playDealer=DealPlay()
		createPlayResult=playDealer.createPlayByREST(playType=playType)#create a play
		playId=createPlayResult["playId"]
		playName=createPlayResult["playName"]
		playDealer.approvePlay(idOfPlay=playId)#approve a play
		playDealer.scorePlay(idOfPlay=playId)#do score
		status=None
		while status != 'Complete':#until score finish
			#time.sleep(20)
			status=playDealer.getStatusOfPlay(idOfPlay=playId)
			print status
			if status=="Error":
				log.error("SCORING FAILED!!!")
				break
			elif status =="Processing":
				log.info("Score is running!")
			time.sleep(10)
		assert status=='Complete'
		selectSQL="SELECT  PreLead_ID  FROM PreLead where Status=1000 and Play_ID=%s"%playId
		numberOfRecommendations=len(DealDB.fetchResultOfSelect(SQL=selectSQL,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True))
		log.info("This play generated %s recommendations "%numberOfRecommendations)
		playLaunchTime=playDealer.launchPlay(nameOfPlayToLaunch=playName)#launch play
		time.sleep(10)
		numberOf2800=len(DealDB.fetchResultOfSelect(SQL="SELECT  PreLead_ID  FROM PreLead where Status=2800 and Play_ID=%s"%playId,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True))
		log.info("numberOf2800 is %s"%numberOf2800)
		assert numberOf2800==numberOfRecommendations
		dlDealer=DataloaderDealer()
		assert dlDealer.isDanteGroupFinishSuccessfully(timePoint=playLaunchTime)
		sfdcDealer=DealSFDC()
		sfdcDealer.loginSF()
		sfdcDealer.resetSFDC()
		sfdcDealer.syncData()
		sfdcDealer.checkRecommendations(playName)
		sfdcDealer.quit()
	def DataFlowForAllTypeOfPlays(self):
		playDealer=DealPlay()
		numberOfRecommendations=0
		playIdList=[]
		playNameList=[]
		playDealer.cleanUpPlaysAndPreleads()
		SalePrismEnvironments.needCleanUpTenantDB=False
		LatticeGeneratesId=None
		#create all play and do score
		#for playType in PlayTypes.t_allTypes:
		for playType in PlayTypes.t_otherTypes:
			createPlayResult=playDealer.createPlayByREST(playType=playType)#create a play
			playId=createPlayResult["playId"]
			assert playId!=None
			if playType == "LatticeGenerates":
				LatticeGeneratesId=playId
			playIdList.append(playId)
			playName=createPlayResult["playName"]
			assert playName!=None
			playNameList.append(playName)
			playDealer.approvePlay(idOfPlay=playId)#approve a play
			if playType !="LatticeGenerates":
				playDealer.scorePlay(idOfPlay=playId)#do score
		SalePrismEnvironments.needCleanUpTenantDB=True
		allScoreFinished=False
		while not allScoreFinished:#until score finish
			scoreStatus=True
			for id in playIdList:
				if id==LatticeGeneratesId:
					continue
				status=playDealer.getStatusOfPlay(idOfPlay=id)
				if status == 'Processing':
					allScoreFinished=False
					log.info("Score is running!%s"%id)
					continue
				elif status=='Error':
					allScoreFinished=False
					scoreStatus=False
					log.error("One Score Failed,Play Id is %s"%id)
					break
				elif status =='Complete':
					allScoreFinished=True
					continue
			if not allScoreFinished:
				if not scoreStatus:
					break
				else:
					time.sleep(20)#wait for another round query
		for id in playIdList:
			selectSQL="SELECT  PreLead_ID  FROM PreLead where Status=1000 and Play_ID=%s"%id
			numberOfRecommendations=numberOfRecommendations+len(DealDB.fetchResultOfSelect(SQL=selectSQL,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True))
		log.info("This play generated %s recommendations "% str(numberOfRecommendations))
		playLaunchTime=playDealer.launchPlay(launchAllPlays=True)#launch all play
		LaunchPlayFinished=False
		print '================='
		while not LaunchPlayFinished:
			print 'start wait launch'
			LaunchedStatus=True
			for id in playIdList:
				print 'play wait launch : %s' % (str(id))
				if id==LatticeGeneratesId:
					continue
				status=playDealer.getLaunchStatus(idOfPlay=id)
				if status == 'Processing':
					LaunchPlayFinished=False
					log.info("Launch is running!%s"%id)
					continue
				elif status=='Error':
					LaunchPlayFinished=False
					LaunchedStatus=False
					log.error("One play launch Failed,Play Id is %s"%id)
					break
				elif status =='Complete':
					LaunchPlayFinished=True
					continue
			if not LaunchPlayFinished:
				if not LaunchedStatus:
					break
				else:
					time.sleep(10)#wait for another round query
		#time.sleep(10)
		numberOf2800=0
		numberOf2500=0
		for playId in playIdList:
			numberOf2800=numberOf2800+len(DealDB.fetchResultOfSelect(SQL="SELECT  PreLead_ID  FROM PreLead where Status=2800 and Play_ID=%s"%playId,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True))
			numberOf2500=numberOf2500+len(DealDB.fetchResultOfSelect(SQL="SELECT  PreLead_ID  FROM PreLead where Status=2500 and Play_ID=%s"%playId,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True))
		print "numberOf2800 is %s"%numberOf2800
		print "numberOf2500 is %s"%numberOf2500
		numberofLaunched=numberOf2500+numberOf2800
		assert numberofLaunched==numberOfRecommendations
		dlDealer=DataloaderDealer()
		#sfdcDealer.configOTK()
		assert dlDealer.isDanteGroupFinishSuccessfully(timePoint=playLaunchTime)
		sfdcDealer=DealSFDC()
		#sfdcDealer.configDanteServer()
		sfdcDealer.loginSF()
		sfdcDealer.resetSFDC()
		sfdcDealer.syncData()
		assert playNameList!=None
		numberofpage=0
		for name in playNameList:
			Num_page_Recommendation=sfdcDealer.checkRecommendations(name)
			numberofpage=numberofpage+Num_page_Recommendation
		sfdcDealer.quit()
		assert numberofpage==numberOf2800

	def test_PLMDataFlow(self):
		if str(SalePrismEnvironments.playType).upper()=='ALL':
			print 'call all type play test'
			self.DataFlowForAllTypeOfPlays()
		else:
			print 'call one type play test'
			self.DataFlowForOnePlay(playType=SalePrismEnvironments.playType)

class EVModelingE2E(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		print 'this is set up method!'
		if SalePrismEnvironments.needSetupEnvironment:
			try:
				print 'set up started'
				setUpEnvironments.setUp()
				SalePrismEnvironments.needSetupEnvironment=False
			except Exception,e:
				log.error('set up failed: '+str(e.message))
				SalePrismEnvironments.needSetupEnvironment=True
	def test_Play_Without_EVModel(self):
		playDealer=DealPlay()
		createPlayResult=playDealer.createPlayByREST(playType=PlayTypes.t_CSRepeatPurchase,UseEVModel=False)#create a play
		playId=createPlayResult["playId"]
		playName=createPlayResult["playName"]
		playDealer.approvePlay(idOfPlay=playId)#approve a play
		playDealer.scorePlay(idOfPlay=playId)#do score
		status=None
		while status != 'Complete':#until score finish
			#time.sleep(20)
			status=playDealer.getStatusOfPlay(idOfPlay=playId)
			print status
			if status=="Error":
				log.error("SCORING FAILED!!!")
				break
			elif status =="Processing":
				log.info("Score is running!")
			time.sleep(10)
		assert status=='Complete'
		selectSQL="SELECT  PreLead_ID  FROM PreLead where Status=1000 and Play_ID=%s"%playId
		numberOfRecommendations=len(DealDB.fetchResultOfSelect(SQL=selectSQL,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True))
		log.info("This play generated %s recommendations "%numberOfRecommendations)
		playLaunchTime=playDealer.launchPlay(nameOfPlayToLaunch=playName)#launch play
		time.sleep(10)
		numberOf2800=len(DealDB.fetchResultOfSelect(SQL="SELECT  PreLead_ID  FROM PreLead where Status=2800 and Play_ID=%s"%playId,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True))
		log.info("numberOf2800 is %s"%numberOf2800)
		assert numberOf2800==numberOfRecommendations
		dlDealer=DataloaderDealer()
		assert dlDealer.isDanteGroupFinishSuccessfully(timePoint=playLaunchTime)
		sfdcDealer=DealSFDC()
		sfdcDealer.loginSF()
		sfdcDealer.resetSFDC()
		sfdcDealer.syncData()
		Num_page_Recommendation=sfdcDealer.checkRecommendations(playName)
		sfdcDealer.quit()
		assert Num_page_Recommendation==numberOfRecommendations,log.error("number recommendation in page is not right, it should be %s, but actually is %s" %(str(numberOfRecommendations),str(Num_page_Recommendation)))


if __name__ == '__main__':
	unittest.main()