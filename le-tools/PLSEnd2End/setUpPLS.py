'''
Created on Mar 25, 2015

@author: smeng
'''


import sys
from Properties import PLSEnvironments
from operations.TestHelpers import PLSConfigRunner
from operations.TestHelpers import DLConfigRunner
from operations.TestHelpers import PretzelRunner
from operations.TestRunner import SessionRunner
import logging


def setUpPls():

    ''' Refresh SVN for templates '''
    print "Refreshing SVN"
    runner = SessionRunner()
    print runner.runCommandLocally("svn update", PLSEnvironments.svn_location_local)

    ''' configure Bard Tenant -- drop templates, configure DL.. '''
    configureBardTenant(PLSEnvironments.pls_bard_1, PLSEnvironments.pls_marketing_app_ELQ)
    configureBardTenant(PLSEnvironments.pls_bard_2, PLSEnvironments.pls_marketing_app_MKTO)




def configureBardTenant(tenant, marketting_app):

    ''' Setting up properties '''
    if tenant == PLSEnvironments.pls_bard_1:
        pls_url = PLSEnvironments.pls_url_1
    elif tenant == PLSEnvironments.pls_bard_2:
        pls_url = PLSEnvironments.pls_url_2
    else:
        logging.error("Tenant provided not found")
        sys.exit("Invalid Tenant")

    ''' Setup Pretzel '''
    print "Running Setup"
    pretzel = PretzelRunner();
    assert pretzel.setupPretzel(marketting_app)

    ''' Configure PLS Credentials '''
    print "for PLS Configuration from UI";
    plsUI = PLSConfigRunner(pls_url);
    print "==>    The PLS URL is: %s" % pls_url;
    plsUI.config(marketting_app);

    ''' configure dataLoader settings '''
    print "configure dataloader settings"
    dlConfig = DLConfigRunner();
    dlConfig.configDLTables(tenant, marketting_app);
    dlConfig.createMockDataProviders(tenant, marketting_app);
    dlConfig.editMockRefreshDataSources(tenant, marketting_app);
    dlConfig.loadCfgTables(tenant, marketting_app);


if __name__ == '__main__':
    setUpPls()