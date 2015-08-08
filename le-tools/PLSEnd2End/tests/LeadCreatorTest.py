'''
Created on Mar 18, 2015

@author: smeng
'''
import unittest
import requests
import json
import datetime
from Properties import PLSEnvironments
from operations.TestHelpers import JamsRunner
from operations.PerformanceHelpers import PerformanceData
from operations.PerformanceHelpers import VisiDBRollBack
from operations.TestHelpers import DanteRunner
from operations.LeadCreator import SFDCRequest
from operations.LeadCreator import EloquaRequest
from operations.LeadCreator import MarketoRequest


class Test(unittest.TestCase):
   
    @unittest.skip("")
    def testJamsCFG(self):
        print "for jams configurations"
        jams = JamsRunner();
        print jams.setJamsTenant(PLSEnvironments.pls_bard_2);

    def testFail(self):
#         assert False, "failing on purpose, expecting to see this text..."
        sss = PLSEnvironments.pls_SFDC_url
        print sss[0:sss.find("services")]

    def testAddAnonymousLeadsToElq(self):
        elq = EloquaRequest()
        resp = elq.addAnonymousContact()
        print resp
        
    def addEloquaContactInCanada(self):
        elq = EloquaRequest()
        resp = elq.addEloquaContact(1,"Canada")
        print resp
        
    def addEloquaContactInUS(self):
        elq = EloquaRequest()
        resp = elq.addEloquaContact(1,"United States")
        print resp   
    
    def addEloquaContactInUK(self):
        elq = EloquaRequest()
        resp = elq.addEloquaContact(1,"UK")
        print resp
        
    def addEloquaContactInCN(self):
        elq = EloquaRequest()
        resp = elq.addEloquaContact(1,"CN")
        print resp          
    
    def testaddLeadToMarketoInUS(self):
        mkto = MarketoRequest();
        print mkto.addLeadToMarketo(1,"United States");
        
    def testaddLeadToMarketoInUK(self):
        mkto = MarketoRequest();
        print mkto.addLeadToMarketo(1,"UK");
        
    def testaddLeadToMarketoInCN(self):
        mkto = MarketoRequest();
        print mkto.addLeadToMarketo(1,"China"); 
        
    def testaddLeadToMarketo(self):
        mkto = MarketoRequest();
        print mkto.addLeadToMarketo(3); 
    def testaddLeadToMarketoForDante(self):
        mkto = MarketoRequest();
        print mkto.addLeadToMarketoForDante(3);   
    def testAddContactsToSFDC(self):
        sfdc = SFDCRequest();
        print sfdc.addContactsToSFDC(3)
           
    def testAddLeadsToSFDCInUS(self):
        sfdc = SFDCRequest();
        print sfdc.addLeadsToSFDC(1,"United States")
        
    def testAddLeadsToSFDCInUK(self):
        sfdc = SFDCRequest();
        print sfdc.addLeadsToSFDC(1,"UK")
        
    def testAddLeadsToSFDCInCN(self):
        sfdc = SFDCRequest();
        print sfdc.addLeadsToSFDC(1,"China")
        
    def testAddLeadsToSFDC(self):
        sfdc = SFDCRequest();
        print sfdc.addLeadsToSFDC(3)
        
    
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()