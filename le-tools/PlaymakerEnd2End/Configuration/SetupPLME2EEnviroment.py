__author__ = 'nxu'
from PlaymakerEnd2End.Configuration.Properties import SalePrismEnvironments
from PlaymakerEnd2End.steps.updateAccount import updateTenantAccount
from PlaymakerEnd2End.steps.configureDataloader import DataloaderDealer
from PlaymakerEnd2End.steps.configureJAMS  import update247DB
from PlaymakerEnd2End.steps.configureSFDC  import DealSFDC
from PlaymakerEnd2End.steps.dealPlay  import DealPlay


if __name__ == "__main__":
    playDealer=DealPlay()
    dlDealer=DataloaderDealer()
    sfdcDealer=DealSFDC()
    #log=SalePrismEnvironments.logProvider.getLog("SetEnvironment",True)
    print '########Start to set up E2E environment###########'
    print 'set up playmaker configuration'
    playDealer.setPlaymakerConfigurationByRest()
    print 'update Tenant Account'
    updateTenantAccount()
    print 'start to set data loader'
    dlDealer.setTenantDataProviderByREST()
    print 'set up Jams'
    update247DB()
    print 'Complete!'

