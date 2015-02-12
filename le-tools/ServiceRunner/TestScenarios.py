#!/usr/local/bin/python
# coding: utf-8

# Base test framework test cases

__author__ = "Illya Vinnichenko"
__copyright__ = "Copyright 2014"
__credits__ = ["Illya Vinnichenko"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "Illya Vinnichenko"
__email__ = "ivinnichenko@lattice-engines.com"
__status__ = "Alpha"

# import modules
import os
#from TestConfigs import ConfigDLC
#from TestRunner import SessionRunner
from TestHelpers import PretzelRunner
from TestHelpers import DLCRunner
from TestHelpers import BardAdminRunner

def testHelperClasses():
    DLCRunner().testRun()

def basePretzelTest():
    bp = "D:\B\spda0__DE-DT-BD_SQ-d__6_9_2_63971nE_2_6_1_63627r_1_3_3_0n\ADEDTBDd69263971nE26163627r1"
    location = "%s\PLS_Templates" % bp
    pretzel = PretzelRunner(host="http://10.41.1.57:5000",
                            svn_location="~/Code/PLS_1.3.3", build_path=bp, specs_path=location)
    pretzel.getMain()
    # Test "EloquaAndSalesforceToEloqua"
    pretzel.testTopology("EloquaAndSalesforceToEloqua")
    # Test "MarketoAndSalesforceToMarketo"
    pretzel.testTopology("MarketoAndSalesforceToMarketo")

def baseDLCTest():
    host = "http://10.41.1.187:5000"
    command = "Launch"
    params = {"-s": "https://10.41.1.187:8080/",
              "-u": "Richard.liu@lattice-engines.com ",
              "-p": "1",
              "-li": "7"
             }
    dlc = DLCRunner(host=host, dlc_path="D:\VisiDB")
    print dlc.runDLCcommand(command, params)
        
#### PLS End-to-End pieces ####


def main():
    print "Welcome to TestScenarios!"
    # Basic Pretzel Test case
    #basePretzelTest()

    # Basic DLC Test case
    #baseDLCTest()

    # Sample DLC unittest
    #DLCRunner().testRun()


if __name__ == '__main__':
    main()