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
        scoring = Scoring(PLSEnvironments.pls_bard_1);
        scoring.runHourlyScoring();
       
    def TestHourlyScoringMKTO(self):
        scoring = Scoring(PLSEnvironments.pls_bard_2);
        scoring.runHourlyScoring();
   
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