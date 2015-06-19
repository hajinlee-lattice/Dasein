'''
Created on Mar 25, 2015

@author: smeng
'''


import sys
from Properties import PLSEnvironments
from operations import PerformanceHelpers
# from operations.TestHelpers import PLSConfigRunner
from operations.TestHelpers_2_0 import PLSConfigRunner
from operations.TestHelpers import DLConfigRunner
from operations.TestHelpers import PretzelRunner
from operations.TestRunner import SessionRunner
from operations.TestHelpers import JamsRunner
import logging


def setUpPls():

    ''' Refresh SVN for templates '''
    print "Refreshing SVN"
    runner = SessionRunner()
    print runner.runCommandLocally("svn update", PLSEnvironments.svn_location_local)

    ''' configure Bard Tenant -- drop templates, configure DL.. '''
    configureBardTenant(PLSEnvironments.pls_bard_1, PLSEnvironments.pls_marketing_app_ELQ)
#     configureBardTenant(PLSEnvironments.pls_bard_3, PLSEnvironments.pls_marketing_app_SFDC)
#     configureBardTenant(PLSEnvironments.pls_bard_2, PLSEnvironments.pls_marketing_app_MKTO)
#     configureBardTenant("leoSFDCTenant_03","SFDC")




def configureBardTenant(tenant, marketting_app):
  
    ''' configure dataLoader settings '''
    print "configure dataloader settings"
    dlConfig = DLConfigRunner();
    dlConfig.configDLTables(tenant, marketting_app);
    dlConfig.createMockDataProviders(tenant, marketting_app);
    dlConfig.editMockRefreshDataSources(tenant, marketting_app);
    dlConfig.loadCfgTables(tenant, marketting_app);

if __name__ == '__main__':
    setUpPls()