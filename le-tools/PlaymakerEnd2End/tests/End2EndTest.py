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
class FullDataFlow(object):
	def DataFlowForOnePlay(self,playType=PlayTypes.t_CSRepeatPurchase,with_EV=False):
		playDealer=DealPlay()
		createPlayResult=playDealer.createPlayByREST(playType=playType,UseEVModel=with_EV)#create a play
		playId=createPlayResult["playId"]
		playName=createPlayResult["playName"]
		playDealer.approvePlay(idOfPlay=playId)#approve a play
		playDealer.scorePlay(idOfPlay=playId)#do score
		status=None
		while status != 'Complete':#until score finish
			#time.sleep(20)
			status=playDealer.getStatusOfPlay(idOfPlay=playId)
			#print status
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
	def DataFlowForAllTypeOfPlays(self,playList=PlayTypes.t_allTypes,with_EV=False):
		playDealer=DealPlay()
		numberOfRecommendations=0
		playIdList=[]
		playNameList=[]
		temp_cleanPlay=SalePrismEnvironments.needCleanUpTenantDB
		if temp_cleanPlay:
			playDealer.cleanUpPlaysAndPreleads()
			SalePrismEnvironments.needCleanUpTenantDB=False
		LatticeGeneratesId=None
		#create all play and do score
		#for playType in PlayTypes.t_allTypes:
		for playType in playList:
			createPlayResult=playDealer.createPlayByREST(playType=playType,UseEVModel=with_EV)#create a play
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
		if temp_cleanPlay:
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
		assert allScoreFinished,log.error('Score Failed because not all play be scored successfully!')
		for id in playIdList:
			selectSQL="SELECT  PreLead_ID  FROM PreLead where Status=1000 and Play_ID=%s"%id
			numberOfRecommendations=numberOfRecommendations+len(DealDB.fetchResultOfSelect(SQL=selectSQL,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True))
		log.info("This play generated %s recommendations "% str(numberOfRecommendations))
		playLaunchTime=playDealer.launchPlay(launchAllPlays=True)#launch all play
		LaunchPlayFinished=False
		#print '================='
		while not LaunchPlayFinished:
			print 'start wait launch'
			LaunchedStatus=True
			for id in playIdList:
				print 'play wait launch : %s' % (str(id))
				if id==LatticeGeneratesId:
					continue
				status=playDealer.getLaunchStatus(idOfPlay=id)
				if status == 'Running':
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
				else:
					LaunchPlayFinished=False
					LaunchedStatus=False
					log.info("Launch of Play '%s' is not start correctly! The return status is '%s'!"% (str(id),str(status)))
			if not LaunchPlayFinished:
				if not LaunchedStatus:
					break
				else:
					time.sleep(10)#wait for another round query
		assert LaunchPlayFinished, log.error('Not All plays be launched successfully!')
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
				SalePrismEnvironments.needSetupEnvironment=True
				assert False,log.error('set up failed: '+str(e.message))
	"""
	"""
	def test_PLMDataFlow(self):
		play_DataFlow=FullDataFlow()
		if str(SalePrismEnvironments.playType).upper()=='ALL':
			print 'call all type play test'
			play_DataFlow.DataFlowForAllTypeOfPlays(playList=PlayTypes.t_otherTypes)
		else:
			print 'call one type play test'
			play_DataFlow.DataFlowForOnePlay(playType=SalePrismEnvironments.playType)

	def test_Play_EVModeling(self):
		play_DataFlow=FullDataFlow()
		if str(SalePrismEnvironments.playType).upper()=='ALL':
			print 'call all type play test'
			if SalePrismEnvironments.useEVModeling:
				play_DataFlow.DataFlowForAllTypeOfPlays(playList=PlayTypes.t_all_EV,with_EV=True)
			else:
				play_DataFlow.DataFlowForAllTypeOfPlays(playList=PlayTypes.t_all_EV,with_EV=False)
		else:
			print 'call one type play test'
			if SalePrismEnvironments.useEVModeling:
				play_DataFlow.DataFlowForOnePlay(playType=SalePrismEnvironments.playType,with_EV=True)
			else:
				play_DataFlow.DataFlowForOnePlay(playType=SalePrismEnvironments.playType,with_EV=False)

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
				SalePrismEnvironments.needSetupEnvironment=True
				assert False,log.error('set up failed: '+str(e.message))
	def test_Play_With_Model(self):
		playDealer=DealPlay()
		createPlayResult=playDealer.createPlayByREST(playType=PlayTypes.t_CSRepeatPurchase,UseEVModel=True)#create a play
		playId=createPlayResult["playId"]
		playName=createPlayResult["playName"]
		selectSQL="SELECT  Scoring_Method,Modeling_Method  FROM Play where Play_ID=%s"%playId
		score_model_method=DealDB.fetchResultOfSelect(SQL=selectSQL,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True)
		log.info("score method is: %s And Model method is: %s "% (score_model_method[0][0],score_model_method[0][1]))
		assert score_model_method[0][0]==3,log.error('the score method with EV modeling should be 3 but actually is:%s' % score_model_method[0][0])
		assert score_model_method[0][1]==1,log.error('the Model method with EV modeling should be 1 but actually is:%s' % score_model_method[0][0])
		#assert 1==2
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
	def test_Play_Without_EVModel(self):
		playDealer=DealPlay()
		createPlayResult=playDealer.createPlayByREST(playType=PlayTypes.t_CSRepeatPurchase,UseEVModel=False)#create a play
		playId=createPlayResult["playId"]
		playName=createPlayResult["playName"]
		selectSQL="SELECT  Scoring_Method,Modeling_Method  FROM Play where Play_ID=%s"%playId
		score_model_method=DealDB.fetchResultOfSelect(SQL=selectSQL,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True)
		log.info("score method is: %s And Model method is: %s "% (score_model_method[0][0],score_model_method[0][1]))
		assert score_model_method[0][0]==2,log.error('the score method with EV modeling should be 3 but actually is:%s' % score_model_method[0][0])
		assert score_model_method[0][1]==0,log.error('the Model method with EV modeling should be 1 but actually is:%s' % score_model_method[0][0])
		#assert 1==2
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
			elif status =="Processing" :
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
	def test_1(self):
		print 'test'


if __name__ == '__main__':
	unittest.main()