'''
Created on Mar 18, 2015

@author: smeng
'''
import unittest
from Properties import PLSEnvironments;
from operations import LeadCreator
from operations import PlsOperations
from operations.LeadCreator import EloquaRequest;
from operations.LeadCreator import MarketoRequest;
from operations.LeadCreator import SFDCRequest;
from operations.TestHelpers import DanteRunner
from test.test_deque import fail


class Test(unittest.TestCase):
    
    def TestHourlyScoringELQ(self):
        elq = EloquaRequest();
        contact_US = elq.addEloquaContact(2,"United States");
        contact_UK = elq.addEloquaContact(2,"UK");
        contact_CN = elq.addEloquaContact(2,"CN");
        
        PlsOperations.runHourlyScoring(PLSEnvironments.pls_bard_1);
               
        contact_lists = elq.getEloquaContact(contact_lists);
        
        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringELQ",contact_US);
        assert len(contact_faileds)==1, contact_faileds;
        
        PlsOperations.runHourlyDanteProcess(PLSEnvironments.pls_bard_1);
       
    def TestHourlyScoringMKTO(self):
        mkto = MarketoRequest();
        leads_US = mkto.addLeadToMarketo(2,"United States");
        leads_UK = mkto.addLeadToMarketo(2,"UK");
        leads_CN = mkto.addLeadToMarketo(2,"China");
        
        PlsOperations.runHourlyScoring(PLSEnvironments.pls_bard_2);
        
        lead_lists = mkto.getLeadFromMarketo(leads_list); 
        
        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringMKTO",leads_US);
        assert len(lead_faileds)==1, lead_faileds;
        
        PlsOperations.runHourlyDanteProcess(PLSEnvironments.pls_bard_2);
    
    def TestHourlyScoringSFDC(self): 
        sfdc = SFDCRequest();
        leads_US = sfdc.addLeadsToSFDC(2,"United States");
        leads_UK = sfdc.addLeadsToSFDC(2,"UK");
        leads_CN = sfdc.addLeadsToSFDC(2,"China");
        contacts_list = sfdc.addContactsToSFDC(2)
        
        PlsOperations.runHourlyScoring(PLSEnvironments.pls_bard_3,PLSEnvironments.pls_marketing_app_SFDC);
        
        lead_lists = sfdc.getLeadsFromSFDC(leads_US); 
        contact_lists = sfdc.getContactsFromSFDC(contacts_list);
        
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC",lead_lists);
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC",contact_lists);
        assert len(lead_faileds)==1, lead_faileds;
        assert len(contact_faileds)==1, contact_faileds; 
        
        PlsOperations.runHourlyDanteProcess(PLSEnvironments.pls_bard_3);


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()