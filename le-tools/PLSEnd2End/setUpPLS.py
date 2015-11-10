'''
Created on Mar 25, 2015

@author: smeng
'''

from Properties import PLSEnvironments
from operations.TestHelpers import LPConfigRunner
from operations.TestRunner import SessionRunner

def setUpPls():
    # ''' Refresh SVN for templates '''
    # print "Refreshing SVN"
    # runner = SessionRunner()
    # if False == runner.runCommandLocally("svn update", PLSEnvironments.svn_location_local):
    #     print "the svn updated failed, please check the really reasons and try again."
    #     return False

    ''' configure Bard Tenant -- drop templates, configure DL.. '''
    lp = LPConfigRunner();

    lp.init(PLSEnvironments.pls_bard_1, PLSEnvironments.pls_marketing_app_ELQ)
    lp.init(PLSEnvironments.pls_bard_2, PLSEnvironments.pls_marketing_app_MKTO)
    lp.init(PLSEnvironments.pls_bard_3, PLSEnvironments.pls_marketing_app_SFDC)

if __name__ == '__main__':
    setUpPls()