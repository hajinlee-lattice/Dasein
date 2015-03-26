'''
Created on Mar 18, 2015

@author: smeng
'''
import unittest
from Properties import PLSEnvironments;
from ServiceRunner.TestHelpers import PLSConfigRunner;
from ServiceRunner.PLSOperations import Models;
from ServiceRunner.TestHelpers import JamsRunner;
from ServiceRunner.PLSOperations import Scoring;
import ServiceRunner.LeadCreator;
from ServiceRunner.LeadCreator import EloquaRequest;
from ServiceRunner.LeadCreator import MarketoRequest;
from ServiceRunner import LeadCreator
from ServiceRunner.LeadCreator import SFDCRequest


class Test(unittest.TestCase):


    def TestBulkScoringELQ(self): 
        elq = EloquaRequest();
        contact_lists = elq.addEloquaContact(15);
               
        scoring = Scoring(PLSEnvironments.pls_bard_1);
        scoring.runBulkScoring();        
              
        contact_lists = elq.getEloquaContact(contact_lists);
        
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringELQ",contact_lists);
        assert len(contact_faileds)==1, contact_faileds;
      
    def TestBulkScoringMKTO(self): 
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketo(15);
        
        scoring = Scoring(PLSEnvironments.pls_bard_2);
        scoring.runBulkScoring();
        
        lead_lists = mkto.getLeadFromMarketo(leads_list); 
        
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringMKTO",lead_lists);
        assert len(lead_faileds)==1, lead_faileds;
    
    def TestHourlyScoringELQ(self):
        elq = EloquaRequest();
        contact_lists = elq.addEloquaContact(3);
        
        scoring = Scoring(PLSEnvironments.pls_bard_1);
        scoring.runHourlyScoring(); 
               
        contact_lists = elq.getEloquaContact(contact_lists);
        
        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringELQ",contact_lists);
        assert len(contact_faileds)==1, contact_faileds;
        
        scoring.runHourlyDanteProcess();
       
    def TestHourlyScoringMKTO(self):
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketo(3);
        
        scoring = Scoring(PLSEnvironments.pls_bard_2);
        scoring.runHourlyScoring();
        
        lead_lists = mkto.getLeadFromMarketo(leads_list); 
        
        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringMKTO",lead_lists);
        assert len(lead_faileds)==1, lead_faileds;
        
        scoring.runHourlyDanteProcess();
        
    def testName(self):
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()