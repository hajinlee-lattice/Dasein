__author__ = 'nxu'
from PlaymakerEnd2End.steps.updateAccount import updateTenantAccount
from PlaymakerEnd2End.steps.configureDataloader import DataloaderDealer
from PlaymakerEnd2End.steps.configureJAMS  import update247DB
from PlaymakerEnd2End.steps.dealPlay  import DealPlay
def setUpEnvironments():
    playDealer=DealPlay()
    dlDealer=DataloaderDealer()
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
if __name__ == "__main__":
    setUpEnvironments()

