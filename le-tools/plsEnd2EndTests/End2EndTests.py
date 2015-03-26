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

    def TestEndToEndELQ(self):
        models = Models();
        models.modelingGenerate(PLSEnvironments.pls_marketing_app_ELQ,PLSEnvironments.pls_url_1);
        
        elq = EloquaRequest();
        contact_lists = elq.addEloquaContact(15);
        scoring = Scoring(PLSEnvironments.pls_bard_1);
        scoring.runBulkScoring();
        contact_lists = elq.getEloquaContact(contact_lists);        
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringELQ",contact_lists);
        assert len(contact_faileds)==1, contact_faileds;
        
        contact_lists = elq.addEloquaContact(3);
        scoring.runHourlyScoring();
        contact_lists = elq.getEloquaContact(contact_lists);        
        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringELQ",contact_lists);
        assert len(contact_faileds)==1, contact_faileds;
        
        scoring.runHourlyDanteProcess();
        
    def TestEndToEndMKTO(self):
        models = Models();
        models.modelingGenerate(PLSEnvironments.pls_marketing_app_MKTO,PLSEnvironments.pls_url_2);
        
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketo(15);        
        scoring = Scoring(PLSEnvironments.pls_bard_2);
        scoring.runBulkScoring();        
        lead_lists = mkto.getLeadFromMarketo(leads_list);         
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringMKTO",lead_lists);
        assert len(lead_faileds)==1, lead_faileds;
        
        leads_list = mkto.addLeadToMarketo(3);
        scoring.runHourlyScoring();        
        lead_lists = mkto.getLeadFromMarketo(leads_list);        
        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringMKTO",lead_lists);
        assert len(lead_faileds)==1, lead_faileds;
        
        scoring.runHourlyDanteProcess();
        
        
    def TestEndToEndELQFromDLConfig(self):
        models = Models();
        models.modelingGenerateFromDLConfig(PLSEnvironments.pls_marketing_app_ELQ,PLSEnvironments.pls_url_1);
        
        elq = EloquaRequest();
        contact_lists = elq.addEloquaContact(15);
        scoring = Scoring(PLSEnvironments.pls_bard_1);
        scoring.runBulkScoring();
        contact_lists = elq.getEloquaContact(contact_lists);        
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringELQ",contact_lists);
        assert len(contact_faileds)==1, contact_faileds;
        
        contact_lists = elq.addEloquaContact(3);
        scoring.runHourlyScoring();
        contact_lists = elq.getEloquaContact(contact_lists);        
        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringELQ",contact_lists);
        assert len(contact_faileds)==1, contact_faileds;
        
        scoring.runHourlyDanteProcess();
        

    def TestEndToEndMKTOFromDLConfig(self):
        models = Models();
        models.modelingGenerateFromDLConfig(PLSEnvironments.pls_marketing_app_MKTO,PLSEnvironments.pls_url_2);
        
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketo(15);        
        scoring = Scoring(PLSEnvironments.pls_bard_2);
        scoring.runBulkScoring();        
        lead_lists = mkto.getLeadFromMarketo(leads_list);         
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringMKTO",lead_lists);
        assert len(lead_faileds)==1, lead_faileds;
        
        leads_list = mkto.addLeadToMarketo(3);
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