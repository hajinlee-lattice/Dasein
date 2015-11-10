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

    def TestBulkScoringELQ(self): 
        elq = EloquaRequest();
        contact_lists = elq.addEloquaContactForDante(5);

        PlsOperations.runBulkScoring(PLSEnvironments.pls_bard_1,PLSEnvironments.pls_marketing_app_ELQ)

        contact_lists = elq.getEloquaContact(contact_lists);

        contact_faileds = LeadCreator.verifyResult("TestBulkScoringELQ",contact_lists);
        assert len(contact_faileds)==1, contact_faileds;
      
    def TestBulkScoringMKTO(self): 
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketoForDante(5);
        
        PlsOperations.runBulkScoring(PLSEnvironments.pls_bard_2,PLSEnvironments.pls_marketing_app_MKTO);
        
        lead_lists = mkto.getLeadFromMarketo(leads_list); 
        
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringMKTO",lead_lists);
        assert len(lead_faileds)==1, lead_faileds;
        
    def TestBulkScoringSFDC(self): 
        sfdc = SFDCRequest();
        leads_list = sfdc.addLeadsToSFDC(5);
        contacts_list = sfdc.addContactsToSFDC(5)
        
        PlsOperations.runBulkScoring(PLSEnvironments.pls_bard_3,PLSEnvironments.pls_marketing_app_SFDC);
        
        lead_lists = sfdc.getLeadsFromSFDC(leads_list); 
        contact_lists = sfdc.getContactsFromSFDC(contacts_list);
        
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC",lead_lists);
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC",contact_lists);
        assert len(lead_faileds)==1, lead_faileds;
        assert len(contact_faileds)==1, contact_faileds;

    def TestHourlyScoringELQ_Dante(self):
        elq = EloquaRequest();
        contact_lists = elq.addEloquaContactForDante(2);

        PlsOperations.runHourlyScoring(PLSEnvironments.pls_bard_1);

        elq_contacts = elq.getEloquaContact(contact_lists[0]);

        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringELQ",elq_contacts);
        print "the length of the contact_faileds is: " % len(contact_faileds)
        assert len(contact_faileds)==1, contact_faileds;

        PlsOperations.runHourlyDanteProcess(PLSEnvironments.pls_bard_1);

        danteLead = contact_lists[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(danteLead)
   
    def TestHourlyScoringMKTO_Dante(self):
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketoForDante(2);
        print "======> prepare test data:";
        print leads_list;

        PlsOperations.runHourlyScoring(PLSEnvironments.pls_bard_2);

        lead_lists = mkto.getLeadFromMarketo(leads_list[0]);

        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringMKTO",lead_lists);
        print "the length of the lead_faileds is: " % len(lead_faileds)
        assert len(lead_faileds)==1, lead_faileds;

        PlsOperations.runHourlyDanteProcess(PLSEnvironments.pls_bard_2);

        danteLead = leads_list[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(danteLead)
        
    def TestHourlyScoringSFDC_Dante(self): 
        sfdc = SFDCRequest();
        leads_list = sfdc.addLeadsToSFDC(2);
        contacts_list = sfdc.addContactsToSFDC(2)
         
        PlsOperations.runHourlyScoring(PLSEnvironments.pls_bard_3,PLSEnvironments.pls_marketing_app_SFDC);
         
        lead_lists = sfdc.getLeadsFromSFDC(leads_list); 
        contact_lists = sfdc.getContactsFromSFDC(contacts_list);
         
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC",lead_lists);
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC",contact_lists);
        print "the length of the lead_faileds is: " % len(lead_faileds)
        print "the length of the contact_faileds is: " % len(contact_faileds)
        assert len(lead_faileds)==1, lead_faileds;
        assert len(contact_faileds) == 1, contact_faileds;

    def testName(self):
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()