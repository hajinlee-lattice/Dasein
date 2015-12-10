__author__ = 'nxu'
from PlaymakerEnd2End.steps.updateAccount import updateTenantAccount
from PlaymakerEnd2End.steps.configureDataloader import DataloaderDealer
from PlaymakerEnd2End.steps.configureJAMS  import update247DB
from PlaymakerEnd2End.steps.dealPlay  import DealPlay
from PlaymakerEnd2End.steps.configureSFDC import DealSFDC
class setUpEnvironments(object):
    haveSetUp=False
    @staticmethod
    def setUp():
        playDealer=DealPlay()
        dlDealer=DataloaderDealer()
        print '########Start to set up E2E environment###########'
        print 'set up playmaker configuration'
        playDealer.setPlaymakerConfigurationByRest()
        print 'update Tenant Account'
        updateTenantAccount()
        print 'start to set data loader'
        dlDealer.setTenantDataProviderByREST()
        print 'set up Jams'
        update247DB()
        print 'start to setup SFDC'
        sfdcDealer=DealSFDC()
        sfdcDealer.configDanteServer()
        sfdcDealer.loginSF()
        sfdcDealer.configOTK()
        sfdcDealer.quit()
        print '######Complete!#######'

#if __name__ == "__main__":
    #setUpEnvironments()

