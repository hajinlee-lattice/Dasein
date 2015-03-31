'''
Created on Mar 18, 2015

@author: smeng
'''
import unittest
from Properties import PLSEnvironments;
from operations.LeadCreator import EloquaRequest;
from operations.LeadCreator import MarketoRequest;
from operations import LeadCreator
from operations import PlsOperations


class Test(unittest.TestCase):

    def TestEndToEndELQ(self):
        PlsOperations.modelingGenerate(PLSEnvironments.pls_marketing_app_ELQ,PLSEnvironments.pls_url_1);
        
        tenant = PLSEnvironments.pls_bard_1
        elq = EloquaRequest();
        contact_lists = elq.addEloquaContact(15);
        PlsOperations.runBulkScoring(tenant);
        contact_lists = elq.getEloquaContact(contact_lists);        
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringELQ",contact_lists);
        assert len(contact_faileds)==1, contact_faileds;
        
        contact_lists = elq.addEloquaContact(3);
        PlsOperations.runHourlyScoring(tenant);
        contact_lists = elq.getEloquaContact(contact_lists);        
        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringELQ",contact_lists);
        assert len(contact_faileds)==1, contact_faileds;
        
        PlsOperations.runHourlyDanteProcess(tenant);


    def TestEndToEndMKTO(self):
        PlsOperations.modelingGenerate(PLSEnvironments.pls_marketing_app_MKTO,PLSEnvironments.pls_url_2);
        
        tenant = PLSEnvironments.pls_bard_2
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketo(15);        
        PlsOperations.runBulkScoring(tenant);        
        lead_lists = mkto.getLeadFromMarketo(leads_list);         
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringMKTO",lead_lists);
        assert len(lead_faileds)==1, lead_faileds;
        
        leads_list = mkto.addLeadToMarketo(3);
        PlsOperations.runHourlyScoring(tenant);        
        lead_lists = mkto.getLeadFromMarketo(leads_list);        
        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringMKTO",lead_lists);
        assert len(lead_faileds)==1, lead_faileds;
        
        PlsOperations.runHourlyDanteProcess(tenant);


    def TestEndToEndELQFromDLConfig(self):
        PlsOperations.modelingGenerateFromDLConfig(PLSEnvironments.pls_marketing_app_ELQ,PLSEnvironments.pls_url_1);
        
        tenant = PLSEnvironments.pls_bard_1
        elq = EloquaRequest();
        contact_lists = elq.addEloquaContact(15);
        PlsOperations.runBulkScoring(tenant);
        contact_lists = elq.getEloquaContact(contact_lists);        
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringELQ",contact_lists);
        assert len(contact_faileds)==1, contact_faileds;
        
        contact_lists = elq.addEloquaContact(3);
        PlsOperations.runHourlyScoring(tenant);
        contact_lists = elq.getEloquaContact(contact_lists);        
        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringELQ",contact_lists);
        assert len(contact_faileds)==1, contact_faileds;
        
        PlsOperations.runHourlyDanteProcess(tenant);
        

    def TestEndToEndMKTOFromDLConfig(self):
        PlsOperations.modelingGenerateFromDLConfig(PLSEnvironments.pls_marketing_app_MKTO,PLSEnvironments.pls_url_2);
        
        tenant = PLSEnvironments.pls_bard_2
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketo(15);        
        PlsOperations.runBulkScoring(tenant);        
        lead_lists = mkto.getLeadFromMarketo(leads_list);         
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringMKTO",lead_lists);
        assert len(lead_faileds)==1, lead_faileds;
        
        leads_list = mkto.addLeadToMarketo(3);
        PlsOperations.runHourlyScoring(tenant);        
        lead_lists = mkto.getLeadFromMarketo(leads_list);        
        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringMKTO",lead_lists);
        assert len(lead_faileds)==1, lead_faileds;
        
        PlsOperations.runHourlyDanteProcess(tenant);
        
    def testName(self):
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()