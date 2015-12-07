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
log=SalePrismEnvironments.log

"""
class DifferentScenario(object):
	def __init__(self):
		playDealer=DealPlay()
	def launchAllPlaysWithDataPlatform(self):
		playDealer.setPlaymakerConfigurationByRest(useDataPlatform="TRUE")
		for f in os.listdir("..\\PlaysCreationJsonFiles"):
			f_Name,f_ext=f.split('.')
			playId=playDealer.createPlayByREST(playType=f_Name,playName=f_Name+"WithDataPlatform")
			playDealer.scorePlay(playId)
			time_spend=0
			#judge the scroe complted, and 1 hour time out
			while status_play!='Complete'or time_spend<3600:
				time.sleep(10)
				time_spend=time_spend+10
				status_play=playDealer.getStatusOfPlay(playId)
			playDealer.approvePlay(playId)
		playDealer.launchPlay(launchAllPlays=True)
	def launchAllPlaysWithoutDataPlatform(self):
		playDealer.setPlaymakerConfigurationByRest(useDataPlatform="FALSE")
		for f in os.listdir("..\\PlaysCreationJsonFiles"):
			f_Name,f_ext=f.split('.')
			playId=playDealer.createPlayByREST(playType=f_Name,playName=f_Name+"WithOUTDataPlatform")
			playDealer.scorePlay(playId)
			status_play=playDealer.getStatusOfPlay(playId)
			time_spend=0
			#judge the scroe complted, and 1 hour time out
			while status_play!='Complete'or time_spend<3600:
				time.sleep(10)
				status_play=playDealer.getStatusOfPlay(playId)
			playDealer.approvePlay(playId)
		playDealer.launchPlay(launchAllPlays=True)
	def scorePlayWithEVModeling(self):
		pass
	def scorePlayWithoutEVModeling(self):
		pass
	def launchSeveralPlays(self):
		pass
playLaunchTime=None
"""
class TestSteps(unittest.TestCase):
	def test_SimpleDataFlowPlaymakerPart(self):
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
		numberOfRecommendations=len(DealDB.fetchResultOfSelect(selectSQL,fetchAll=True))
		log.info("This play generated %s recommendations "%numberOfRecommendations)
		playLaunchTime=playDealer.launchPlay(nameOfPlayToLaunch=playName)#launch play
		time.sleep(10)
		numberOf2800=len(DealDB.fetchResultOfSelect(SQL="SELECT  PreLead_ID  FROM PreLead where Status=2800 and Play_ID=%s"%playId,fetchAll=True))
		print "numberOf2800 is %s"%numberOf2800
		assert numberOf2800==numberOfRecommendations
		dlDealer=DataloaderDealer()
		sfdcDealer=DealSFDC()
		sfdcDealer.loginSF()
		sfdcDealer.resetSFDC()
		sfdcDealer.configDanteServer()
		sfdcDealer.configOTK()
		#assert dlDealer.isDanteGroupFinishSuccessfully(timePoint=playLaunchTime)
		sfdcDealer.syncData()
		sfdcDealer.checkRecommendations(playName)
		sfdcDealer.quit()
if __name__ == '__main__':
	unittest.main()