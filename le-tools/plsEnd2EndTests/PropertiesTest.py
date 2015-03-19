'''
Created on Mar 18, 2015

@author: smeng
'''
import unittest

from Properties import PLSEnvironments
from ServiceRunner.TestHelpers import JamsRunner

class Test(unittest.TestCase):

    
    def testPLSEnvironmentsPrint(self):
        print "the first try on this: ";
        print PLSEnvironments.pls_server;
        print PLSEnvironments.pls_server_folder;
        print PLSEnvironments.pls_bard_1;
        print PLSEnvironments.pls_bard_2;
        print PLSEnvironments.pls_url_1;
        print PLSEnvironments.pls_url_2;
        print PLSEnvironments.pls_pretzel;
        print PLSEnvironments.pls_bardAdminTool_1;
        print PLSEnvironments.pls_bardAdminTool_2;
        print PLSEnvironments.pls_db_server;
        print PLSEnvironments.pls_db_ScoringDaemon;        
    
    def testJamsCFG(self):
        print "for jams configurations"
        jams = JamsRunner();
        print jams.setJamsTenant(PLSEnvironments.pls_bard_2);
    
        


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()