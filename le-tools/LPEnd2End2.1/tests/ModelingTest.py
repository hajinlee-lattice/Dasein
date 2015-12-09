'''
Created on Mar 12, 2015

@author: smeng
'''
import unittest
from Properties import PLSEnvironments
from operations import PlsOperations
from operations.TestHelpers import LPConfigRunner


class Test(unittest.TestCase):

    def testModelingEloqua(self):
        lp = LPConfigRunner();
        PlsOperations.runModelingLoadGroups(PLSEnvironments.pls_bard_1, PLSEnvironments.pls_marketing_app_ELQ);
        '''activate the inital model for the new tenant'''
        if False == lp.lpActivateModel(PLSEnvironments.pls_bard_1):
            print "there is no new model been activated"
            assert False;

    def testModelingMarketo(self):
        lp = LPConfigRunner();
        PlsOperations.runModelingLoadGroups(PLSEnvironments.pls_bard_2, PLSEnvironments.pls_marketing_app_MKTO);
        '''activate the inital model for the new tenant'''
        if False == lp.lpActivateModel(PLSEnvironments.pls_bard_2):
            print "there is no new model been activated"
            assert False;
        
    def testModelingSFDC(self):
        lp = LPConfigRunner();
        PlsOperations.runModelingLoadGroups(PLSEnvironments.pls_bard_3, PLSEnvironments.pls_marketing_app_SFDC);
        '''activate the inital model for the new tenant'''
        if False == lp.lpActivateModel(PLSEnvironments.pls_bard_3):
            print "there is no new model been activated"
            assert False;


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()