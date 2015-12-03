"""
@author bwang
@createDate 11/11/2015
""" 
import sys,os
sys.path.append("..")
from Configuration.Properties import SalePrismEnvironments
from steps.updateAccount import updateTenantAccount
from steps.configureDataloader import DataloaderDealer
from steps.configureJAMS  import update247DB
from steps.configureSFDC  import DealSFDC
from steps.dealPlay  import DealPlay
import unittest,time

class End2EndNecessarySteps(object):
	def __init__(self):
		self.playDealer=DealPlay()
		self.dlDealer=DataloaderDealer()
		self.sfdcDealer=DealSFDC()
	def PlayMakerConfiguration(self):
		self.playDealer.setPlaymakerConfigurationByRest()
	def MatchAccountID(self):
		updateTenantAccount()
	def ConfigureDataLoader(self):
		self.dlDealer.setTenantDataProviderByREST()
	def ConfigureJAMS(self):
		update247DB()
	def dataFlowPlaymakerPart(self):
		PlayID=self.playDealer.createPlayByREST()#create a play
		self.playDealer.approvePlay(idOfPlay=PlayID)#approve a play
		self.playDealer.scorePlay(idOfPlay=PlayID)#do score
		status=self.playDealer.getStatusOfPlay(idOfPlay=PlayID)
		while status != 'Complete':#until score finish
			time.sleep(10)
			status=self.playDealer.getStatusOfPlay(idOfPlay=PlayID)
		self.playDealer.launchPlay()#launch play
	def dataFlowSFDCPart(self):
		self.sfdcDealer.loginSF()
		self.sfdcDealer.configDanteServer()
		self.sfdcDealer.configOTK()
		if not self.dlDealer.isDanteGroupFinishSuccessfully():
			return
		self.sfdcDealer.syncData()
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
		self.steps=End2EndNecessarySteps()
	def test_aPlayMakerConfiguration(self):
		self.steps.PlayMakerConfiguration()
	def test_cMatchAccountID(self):
		self.steps.MatchAccountID()
	def test_dConfigureDataLoader(self):
		self.steps.ConfigureDataLoader()
	def test_eConfigureJAMS(self):
		self.steps.ConfigureJAMS()
	def test_fPlayPart(self):
		self.steps.dataFlowPlaymakerPart()
	def test_hSalesforcePart(self):
		self.steps.dataFlowSFDCPart()

if __name__ == '__main__':
	unittest.main()
	SalePrismEnvironments.ff.quit()