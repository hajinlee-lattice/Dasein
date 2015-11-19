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
        assert len(contact_faileds)==0, contact_faileds;

        danteLead = contact_lists[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(PLSEnvironments.pls_bard_1,danteLead)
      
    def TestBulkScoringMKTO(self): 
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketoForDante(5);
        
        PlsOperations.runBulkScoring(PLSEnvironments.pls_bard_2,PLSEnvironments.pls_marketing_app_MKTO);
        
        lead_lists = mkto.getLeadFromMarketo(leads_list); 
        
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringMKTO",lead_lists);
        assert len(lead_faileds)==0, lead_faileds;

        danteLead = leads_list[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(PLSEnvironments.pls_bard_2,danteLead)
        
    def TestBulkScoringSFDC(self): 
        sfdc = SFDCRequest();
        leads_list = sfdc.addLeadsToSFDC(5);
        contacts_list = sfdc.addContactsToSFDC(5)
        
        PlsOperations.runBulkScoring(PLSEnvironments.pls_bard_3,PLSEnvironments.pls_marketing_app_SFDC);
        
        lead_lists = sfdc.getLeadsFromSFDC(leads_list); 
        contact_lists = sfdc.getContactsFromSFDC(contacts_list);
        
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC",lead_lists);
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC",contact_lists);
        assert len(lead_faileds)==0, lead_faileds;
        assert len(contact_faileds)==0, contact_faileds;

        danteLead = leads_list[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(PLSEnvironments.pls_bard_3,danteLead)
        danteLead = contact_lists[2].values()[0]
        dr.checkDanteValue(PLSEnvironments.pls_bard_3,danteLead)

    def TestHourlyScoringELQ_Dante(self):
        elq = EloquaRequest();
        contact_lists = elq.addEloquaContactForDante(2);
        # contact_lists=[{u'405116': 'd28tegAvBpnWjUDxmR7TEwFYKrs1yPaQhIJ34GL9bqoi@tellabs.com', u'405115': 'LVKrWIGp5ycRPowJmuOlZ49e0zDdqkstj1f@southsubsynthetics.com'}, {'d28tegAvBpnWjUDxmR7TEwFYKrs1yPaQhIJ34GL9bqoi@tellabs.com': u'0038000001sCIYJAA4', 'LVKrWIGp5ycRPowJmuOlZ49e0zDdqkstj1f@southsubsynthetics.com': u'0038000001sCIYEAA4'}, {'d28tegAvBpnWjUDxmR7TEwFYKrs1yPaQhIJ34GL9bqoi@tellabs.com': u'00Q8000001cSUylEAG', 'LVKrWIGp5ycRPowJmuOlZ49e0zDdqkstj1f@southsubsynthetics.com': u'00Q8000001cSUygEAG'}]
        PlsOperations.runHourlyScoring(PLSEnvironments.pls_bard_1);

        elq_contacts = elq.getEloquaContact(contact_lists[0]);

        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringELQ",elq_contacts);
        assert len(contact_faileds)==0, contact_faileds;

        danteLead = contact_lists[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(PLSEnvironments.pls_bard_1,danteLead)
   
    def TestHourlyScoringMKTO_Dante(self):
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketoForDante(2);
        print "======> prepare test data:";
        print leads_list;

        PlsOperations.runHourlyScoring(PLSEnvironments.pls_bard_2);

        lead_lists = mkto.getLeadFromMarketo(leads_list[0]);

        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringMKTO",lead_lists);
        assert len(lead_faileds)==0, lead_faileds;

        danteLead = leads_list[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(PLSEnvironments.pls_bard_2,danteLead)
        
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
        assert len(lead_faileds)==0, lead_faileds;
        assert len(contact_faileds) == 0, contact_faileds;

        danteLead = leads_list[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(PLSEnvironments.pls_bard_3,danteLead)
        danteLead = contact_lists[2].values()[0]
        dr.checkDanteValue(PLSEnvironments.pls_bard_3,danteLead)

    def testName(self):
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()