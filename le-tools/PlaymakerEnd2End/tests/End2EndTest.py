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
		if not setUpEnvironments.haveSetUp:
			try:
				print 'set up started'
				setUpEnvironments.setUp()
				setUpEnvironments.haveSetUp=True
			except Exception,e:
				log.error('set up failed: '+str(e.message))
				setUpEnvironments.haveSetUp=False
	def test_SimpleDataFlow(self):
		playDealer=DealPlay()
		createPlayResult=playDealer.createPlayByREST()#create a play
		playId=createPlayResult["playId"]
		playName=createPlayResult["playName"]
		playDealer.approvePlay(idOfPlay=playId)#approve a play
		playDealer.scorePlay(idOfPlay=playId)#do score
		status=None
		while status != 'Complete':#until score finish
			time.sleep(20)
			status=playDealer.getStatusOfPlay(idOfPlay=playId)
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

	def test_CreateAllTypeOfPlays(self):
		playDealer=DealPlay()
		numberOfRecommendations=0
		playIdList=[]
		playNameList=[]
		LatticeGeneratesId=None
		#create all play and do score
		for playType in PlayTypes.t_allTypes:
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
		allScoreFinished=False
		while not allScoreFinished:#until score finish
			for id in playIdList:
				if id==LatticeGeneratesId:
					continue
				status=playDealer.getStatusOfPlay(idOfPlay=id)
				if status != 'Complete':#until score finish
					allScoreFinished=False
					break
				elif status=='Complete':
					allScoreFinished=True
			if not allScoreFinished:
				time.sleep(20)#wait for another round query
		for id in playIdList:
			selectSQL="SELECT  PreLead_ID  FROM PreLead where Status=1000 and Play_ID=%s"%id
			numberOfRecommendations=numberOfRecommendations+len(DealDB.fetchResultOfSelect(SQL=selectSQL,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True))
		log.info("This play generated %s recommendations "%numberOfRecommendations)
		playLaunchTime=playDealer.launchPlay(launchAllPlays=True)#launch all play
		time.sleep(10)
		numberOf2800=0
		for playId in playIdList:
			numberOf2800=numberOf2800+len(DealDB.fetchResultOfSelect(SQL="SELECT  PreLead_ID  FROM PreLead where Status=2800 and Play_ID=%s"%playId,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True))
		print "numberOf2800 is %s"%numberOf2800
		assert numberOf2800==numberOfRecommendations
		dlDealer=DataloaderDealer()
		#sfdcDealer.configOTK()
		assert dlDealer.isDanteGroupFinishSuccessfully(timePoint=playLaunchTime)
		sfdcDealer=DealSFDC()
		#sfdcDealer.configDanteServer()
		sfdcDealer.loginSF()
		sfdcDealer.resetSFDC()
		sfdcDealer.syncData()
		assert playNameList!=None
		for name in playNameList:
			sfdcDealer.checkRecommendations(name)
		sfdcDealer.quit()

class EVModelingE2E(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		print 'this is set up method!'
		if not setUpEnvironments.haveSetUp:
			try:
				print 'set up started'
				setUpEnvironments.setUp()
				setUpEnvironments.haveSetUp=True
			except Exception,e:
				log.error('set up failed: '+str(e.message))
				setUpEnvironments.haveSetUp=False
	def test_Play_Without_EVModel(self):
		playDealer=DealPlay()
		createPlayResult=playDealer.createPlayByREST(playType=PlayTypes.t_CSRepeatPurchase,UseEVModel=False)#create a play
		playId=createPlayResult["playId"]
		playName=createPlayResult["playName"]
		playDealer.approvePlay(idOfPlay=playId)#approve a play
		playDealer.scorePlay(idOfPlay=playId)#do score
		status=None
		while status != 'Complete':#until score finish
			time.sleep(20)
			status=playDealer.getStatusOfPlay(idOfPlay=playId)
		selectSQL="SELECT  PreLead_ID  FROM PreLead where Status=1000 and Play_ID=%s"%playId
		numberOfRecommendations=len(DealDB.fetchResultOfSelect(SQL=selectSQL,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True))
		log.info("This play generated %s recommendations "%numberOfRecommendations)
		playLaunchTime=playDealer.launchPlay(nameOfPlayToLaunch=playName)#launch play
		time.sleep(10)
		numberOf2800=len(DealDB.fetchResultOfSelect(SQL="SELECT  PreLead_ID  FROM PreLead where Status=2800 and Play_ID=%s"%playId,SERVER=SalePrismEnvironments.tenantDBUrl,DATABASE=SalePrismEnvironments.tenantName,UID=SalePrismEnvironments.tenantDBUser,PWD=SalePrismEnvironments.tenantDBPassword,fetchAll=True))
		log.info("numberOf2800 is %s"%numberOf2800)
		assert numberOf2800==numberOfRecommendations
		dlDealer=DataloaderDealer()
		#sfdcDealer.configOTK()
		assert dlDealer.isDanteGroupFinishSuccessfully(timePoint=playLaunchTime)
		sfdcDealer=DealSFDC()
		#sfdcDealer.configDanteServer()
		sfdcDealer.loginSF()
		sfdcDealer.resetSFDC()
		sfdcDealer.syncData()
		sfdcDealer.checkRecommendations(playName)
		sfdcDealer.quit()


if __name__ == '__main__':
	unittest.main()