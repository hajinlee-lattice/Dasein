#!/usr/local/bin/python
# coding: utf-8
'''
Created on 2015年2月28日

@author: GLiu
'''
from Env.Properties import PLSEnvironments;
from BasicOperations.TestHelpers import PLSConfigRunner;
from BasicOperations.Operations import Models;
from BasicOperations.TestHelpers import JamsRunner;
from BasicOperations.Operations import Scoring;
import BasicOperations.LeadCreator;
from BasicOperations.LeadCreator import EloquaRequest;
from BasicOperations.LeadCreator import MarketoRequest;
from BasicOperations import LeadCreator
from BasicOperations.LeadCreator import SFDCRequest

class TestEndToEndPLS(object):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
      

    def TestModelingGenerageELQ(self):        
        models = Models();
        models.modelingGenerate(PLSEnvironments.pls_marketing_app_ELQ,PLSEnvironments.pls_url_1);
      
    def TestModelingGenerageMKTO(self):        
        models = Models();
        models.modelingGenerate(PLSEnvironments.pls_marketing_app_MKTO,PLSEnvironments.pls_url_2); 
        
    def TestBulkScoringELQ(self):        
        scoring = Scoring(PLSEnvironments.pls_bard_1);
        scoring.runBulkScoring();
      
    def TestBulkScoringMKTO(self): 
        scoring = Scoring(PLSEnvironments.pls_bard_2);
        scoring.runBulkScoring();
    
    def TestHourlyScoringELQ(self):
        elq = EloquaRequest();
        contact_lists = elq.addEloquaContact(5);
        scoring = Scoring(PLSEnvironments.pls_bard_1);
        scoring.runHourlyScoring();
        
        print elq.getEloquaContact(contact_lists);
       
    def TestHourlyScoringMKTO(self):
        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketo(3);
        scoring = Scoring(PLSEnvironments.pls_bard_2);
        scoring.runHourlyScoring();
        print mkto.getLeadFromMarketo(leads_list); 
   
    def TestEndToEndELQ(self):
        models = Models();
        models.modelingGenerate(PLSEnvironments.pls_marketing_app_ELQ,PLSEnvironments.pls_url_1);
        scoring = Scoring(PLSEnvironments.pls_bard_1);
        scoring.runBulkScoring();
        scoring.runHourlyScoring();
        
    def TestEndToEndMKTO(self):
        models = Models();
        models.modelingGenerate(PLSEnvironments.pls_marketing_app_MKTO,PLSEnvironments.pls_url_2);
        scoring = Scoring(PLSEnvironments.pls_bard_2);
        scoring.runBulkScoring();
        scoring.runHourlyScoring();
        
        
    def TestEndToEndELQFromDLConfig(self):
        models = Models();
        models.modelingGenerateFromDLConfig(PLSEnvironments.pls_marketing_app_ELQ,PLSEnvironments.pls_url_1);
        scoring = Scoring(PLSEnvironments.pls_bard_1);
        scoring.runBulkScoring();
        scoring.runHourlyScoring();
    def TestEndToEndMKTOFromDLConfig(self):
        models = Models();
        models.modelingGenerateFromDLConfig(PLSEnvironments.pls_marketing_app_MKTO,PLSEnvironments.pls_url_2);
        scoring = Scoring(PLSEnvironments.pls_bard_2);
        scoring.runBulkScoring();
        scoring.runHourlyScoring();
    
class TestProperties(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
      

    def TestPLSEnvironmentsPrint(self):
        print "the first try on this: ";
        print PLSEnvironments.pls_server;
        print PLSEnvironments.pls_server_folder;
        print PLSEnvironments.pls_url_1;
        print PLSEnvironments.pls_url_2;
        print PLSEnvironments.pls_pretzel;
        print PLSEnvironments.pls_bardAdminTool_1;
        print PLSEnvironments.pls_bardAdminTool_2;
        print PLSEnvironments.pls_db_server;
        print PLSEnvironments.pls_db_ScoringDaemon;        
    
    def TestJamsCFG(self):
        print "for jams configurations"
        jams = JamsRunner();
        print jams.setJamsTenant(PLSEnvironments.pls_bard_2);
    def TestOperations(self):
        model = Models(); 
        model.activateModel(PLSEnvironments.pls_bardAdminTool_1, PLSEnvironments.pls_bard_1);     
        
    def TestPLSConfiguration(self):
        print "for PLS Configuration from UI";
        plsUI = PLSConfigRunner(self);
        print PLSEnvironments.pls_url_2;
        plsUI.config(PLSEnvironments.pls_url_2);
        

class TestLeadCreator(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
      

    def TestMetaDataPrint(self):
        print "the first try on this: ";
        print BasicOperations.LeadCreator.getDomains(PLSEnvironments.pls_marketing_app_ELQ);
        print BasicOperations.LeadCreator.getAddresses();
        print BasicOperations.LeadCreator.getActivityTypes();
        print BasicOperations.LeadCreator.getStageNames();
        print BasicOperations.LeadCreator.getTitles();  
        
    def TestEloquaDataCreate(self):
        elq = EloquaRequest();
        elq.addEloquaContact(5);
    def TestEloquaDataGet(self):
#         elq = EloquaRequest();
#         contact_ids={};
#         contact_ids["403636"]="F97F534F@pwbonline.com";
#         contact_ids["403637"]="F97F534F@pwbonline.com";
#         contact_ids["403638"]="F97F534F@pwbonline.com";
#         contact_ids["403639"]="F97F534F@pwbonline.com";
#         elq.getEloquaContact(contact_ids);
        LeadCreator.getSequence();
    
    def TestEloquaDataDelete(self):
        elq = EloquaRequest();
        contact_ids={};
        contact_ids["403636"]="F97F534F@pwbonline.com";
        contact_ids["403637"]="F97F534F@pwbonline.com";
        contact_ids["403638"]="F97F534F@pwbonline.com";
        contact_ids["403639"]="F97F534F@pwbonline.com";
        elq.deleteContact("403635");
           
    def TestMarketoDataCreate(self):
        mkto = MarketoRequest();
        mkto.addLeadToMarketo(3);
        
    def TestMarketoDataGet(self):
        mkto = MarketoRequest();
        contact_ids={};
        contact_ids["1011700"]="F97F534F@pwbonline.com";
        contact_ids["1011702"]="F97F534F@pwbonline.com";
        contact_ids["1011703"]="F97F534F@pwbonline.com";
        contact_ids["1011704"]="F97F534F@pwbonline.com";
        mkto.getLeadFromMarketo(contact_ids);  
        
    def TestMarketoDataDelete(self):
        mkto = MarketoRequest();
        contact_ids={};
        contact_ids["1011705"]="F97F534F@pwbonline.com";
        contact_ids["1011702"]="F97F534F@pwbonline.com";
        contact_ids["1011703"]="F97F534F@pwbonline.com";
        contact_ids["1011704"]="F97F534F@pwbonline.com";
        print mkto.delete("1011700").text;

    def TestSFDC(self):
        sfdc = SFDCRequest();
#         print sfdc.updateAccountToSFDC("0018000001IDmSOAA1", name="newTestForIt")
#         print sfdc.deleteAccount("0018000001IDmSOAA1");
        print sfdc.addOpportunityToSFDC();


