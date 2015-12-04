"""
@author bwang
@createDate 11/11/2015
""" 
import os
from PlaymakerEnd2End.Configuration.Properties import SalePrismEnvironments
from PlaymakerEnd2End.steps.updateAccount import updateTenantAccount
from PlaymakerEnd2End.steps.configureDataloader import DataloaderDealer
from PlaymakerEnd2End.steps.configureJAMS  import update247DB
from PlaymakerEnd2End.steps.configureSFDC  import DealSFDC
from PlaymakerEnd2End.steps.dealPlay  import DealPlay
from PlaymakerEnd2End.steps.DBHelper import DealDB
import unittest,time
log=SalePrismEnvironments.log


class DifferentScenario(object):
	def __init__(self):
		self.playDealer=DealPlay()
	def launchAllPlaysWithDataPlatform(self):
		self.playDealer.setPlaymakerConfigurationByRest(useDataPlatform="TRUE")
		for f in os.listdir("..\\PlaysCreationJsonFiles"):
			playId=self.playDealer.createPlayByREST(playType=f,playName=f+"WithDataPlatform")
			self.playDealer.scorePlay(playId)
			self.playDealer.approvePlay(playId)
		self.playDealer.launchPlay(launchAllPlays=True)
	def launchAllPlaysWithoutDataPlatform(self):
		self.playDealer.setPlaymakerConfigurationByRest(useDataPlatform="FALSE")
		for f in os.listdir("..\\PlaysCreationJsonFiles"):
			playId=self.playDealer.createPlayByREST(playType=f,playName=f+"WithOUTDataPlatform")
			self.playDealer.scorePlay(playId)
			self.playDealer.approvePlay(playId)
		self.playDealer.launchPlay(launchAllPlays=True)
	def scorePlayWithEVModeling(self):
		pass
	def scorePlayWithoutEVModeling(self):
		pass
	def launchSeveralPlays(self):
		pass
class TestSteps(unittest.TestCase):
	def setUp(self):
		self.playDealer=DealPlay()
		self.dlDealer=DataloaderDealer()
		self.sfdcDealer=DealSFDC()
		self.playName=None
		self.playId=None
	def test_SimpleDataFlowPlaymakerPart(self):
		createPlayResult=self.playDealer.createPlayByREST()#create a play
		self.playId=createPlayResult["playId"]
		self.playName=createPlayResult["playName"]
		self.numberOfRecommendations=createPlayResult["recommendationsNumberGenerated"]
		self.playDealer.approvePlay(idOfPlay=self.playId)#approve a play
		self.playDealer.scorePlay(idOfPlay=self.playId)#do score
		status=self.playDealer.getStatusOfPlay(idOfPlay=self.playId)
		while status != 'Complete':#until score finish
			time.sleep(10)
			status=self.playDealer.getStatusOfPlay(idOfPlay=self.playId)
		self.playDealer.launchPlay(nameOfPlayToLaunch=self.playName)#launch play
		numberOf2800=DealDB.fetchAllResultOfSelect(SQL="SELECT  PreLead_ID  FROM PreLead where Status=2800 and Play_ID=%s"%self.playId)
		assert numberOf2800==self.numberOfRecommendations
	def test_SimpleDataFlowSFDCPart(self):
		self.sfdcDealer.loginSF()
		self.sfdcDealer.configDanteServer()
		self.sfdcDealer.configOTK()
		assert self.dlDealer.isDanteGroupFinishSuccessfully()
		self.sfdcDealer.syncData()
		#self.sfdcDealer.checkRecommendations(self.playName)
if __name__ == '__main__':
	unittest.main()
	SalePrismEnvironments.ff.quit()