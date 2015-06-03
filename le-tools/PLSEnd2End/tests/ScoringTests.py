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
from operations.TestHelpers import DanteRunner


class Test(unittest.TestCase):


    def TestBulkScoringELQ(self): 
        elq = EloquaRequest();
        contact_lists = elq.addEloquaContact(15);

        PlsOperations.runBulkScoring(PLSEnvironments.pls_bard_1)

        contact_lists = elq.getEloquaContact(contact_lists);

        contact_faileds = LeadCreator.verifyResult("TestBulkScoringELQ",contact_lists);
        assert len(contact_faileds)==1, contact_faileds;
      
    def TestBulkScoringMKTO(self): 
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketo(15);
        
        PlsOperations.runBulkScoring(PLSEnvironments.pls_bard_2);
        
        lead_lists = mkto.getLeadFromMarketo(leads_list); 
        
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringMKTO",lead_lists);
        assert len(lead_faileds)==1, lead_faileds;
    
    def TestHourlyScoringELQ(self):
        elq = EloquaRequest();
        contact_lists = elq.addEloquaContact(3);
        
        PlsOperations.runHourlyScoring(PLSEnvironments.pls_bard_1);
               
        contact_lists = elq.getEloquaContact(contact_lists);
        
        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringELQ",contact_lists);
        assert len(contact_faileds)==1, contact_faileds;
        
        PlsOperations.runHourlyDanteProcess(PLSEnvironments.pls_bard_1);
       
    def TestHourlyScoringMKTO(self):
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketo(3);
        
        PlsOperations.runHourlyScoring(PLSEnvironments.pls_bard_2);
        
        lead_lists = mkto.getLeadFromMarketo(leads_list); 
        
        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringMKTO",lead_lists);
        assert len(lead_faileds)==1, lead_faileds;
        
        PlsOperations.runHourlyDanteProcess(PLSEnvironments.pls_bard_2);
     
    def TestHourlyScoringELQ_Dante(self):
        elq = EloquaRequest();
        contact_lists = elq.addEloquaContactForDante(3);        
                
        PlsOperations.runHourlyScoring(PLSEnvironments.pls_bard_1); 
                  
        elq_contacts = elq.getEloquaContact(contact_lists[0]);
           
        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringELQ",elq_contacts);
        assert len(contact_faileds)==1, contact_faileds;
            
        PlsOperations.runHourlyDanteProcess(PLSEnvironments.pls_bard_1);        
        
        danteLead = contact_lists[2].values()[0]        
        dr = DanteRunner()
        dr.checkDanteValue(danteLead)
   
    def TestHourlyScoringMKTO_Dante(self):
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketoForDante(3);
        print "======> prepare test data:";
        print leads_list;
        
        PlsOperations.runHourlyScoring(PLSEnvironments.pls_bard_2);
         
        lead_lists = mkto.getLeadFromMarketo(leads_list[0]); 
         
        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringMKTO",lead_lists);
        assert len(lead_faileds)==1, lead_faileds;
         
        PlsOperations.runHourlyDanteProcess(PLSEnvironments.pls_bard_2);   
        
        danteLead = leads_list[2].values()[0]        
        dr = DanteRunner()
        dr.checkDanteValue(danteLead)
    def testName(self):
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()