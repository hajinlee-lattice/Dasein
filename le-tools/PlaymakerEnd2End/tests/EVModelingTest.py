__author__ = 'nxu'
import unittest
import time

from PlaymakerEnd2End.Configuration.Properties import SalePrismEnvironments
from PlaymakerEnd2End.steps.configureDataloader import DataloaderDealer
from PlaymakerEnd2End.steps.dealPlay  import DealPlay
from PlaymakerEnd2End.tools.DBHelper import DealDB
from PlaymakerEnd2End.steps.dealPlay import PlayTypes
from PlaymakerEnd2End.steps.configureSFDC import DealSFDC
log=SalePrismEnvironments.log

class EVModelingE2E(unittest.TestCase):
	def test_Play_With_EVModel(self):
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
		sfdcDealer=DealSFDC()
		sfdcDealer.configDanteServer()
		sfdcDealer.loginSF()
		sfdcDealer.resetSFDC()
		sfdcDealer.configOTK()
		#assert dlDealer.isDanteGroupFinishSuccessfully(timePoint=playLaunchTime)
		sfdcDealer.syncData()
		sfdcDealer.checkRecommendations(playName)
		sfdcDealer.quit()

if __name__ == '__main__':
	unittest.main()