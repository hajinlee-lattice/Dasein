'''
Created on Mar 18, 2015

@author: smeng
'''
import unittest
from Properties import PLSEnvironments;
from operations.LeadCreator import EloquaRequest;
from operations.LeadCreator import MarketoRequest;
from operations.LeadCreator import SFDCRequest;
from operations.TestHelpers import DanteRunner
from operations import LeadCreator
from operations import PlsOperations
import time
from operations.TestHelpers import LPConfigRunner


class Test(unittest.TestCase):

    def TestEndToEndELQ(self):
        ''' prepare the Tenant -- drop templates, configure DL.. '''
        tenant = "AutoJekinsElq_%s_%s" % (time.strftime('%m_%d'), int(time.time()))
        lp = LPConfigRunner();
        lp.init(tenant, PLSEnvironments.pls_marketing_app_ELQ)
        PlsOperations.runModelingLoadGroups(tenant, PLSEnvironments.pls_marketing_app_ELQ);

        '''activate the inital model for the new tenant'''
        if False == lp.lpActivateModel(tenant):
            print "there is no new model been activated"
            assert False;

        elq = EloquaRequest();
        contact_lists = elq.addEloquaContactForDante(5);
        PlsOperations.runBulkScoring(tenant);
        time.sleep(15)
        contact_lists = elq.getEloquaContact(contact_lists[0]);
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringELQ",contact_lists);
        assert len(contact_faileds)==0, contact_faileds;

        danteLead = contact_lists[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant,danteLead)

        contact_lists = elq.addEloquaContactForDante(2);
        PlsOperations.runHourlyScoring(tenant);
        time.sleep(15)
        contact_lists = elq.getEloquaContact(contact_lists[0]);
        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringELQ",contact_lists);
        assert len(contact_faileds)==0, contact_faileds;

        '''verify the dante result, check the leadcache table in dante database directly.'''
        # waitting to implement #dataplatforms
        danteLead = contact_lists[2].values()[0]
        dr.checkDanteValue(tenant,danteLead)

    def TestEndToEndMKTO(self):
        ''' prepare the Tenant -- drop templates, configure DL.. '''
        tenant = "AutoJekinsMKTO_%s_%s" % (time.strftime('%m_%d'), int(time.time()))
        lp = LPConfigRunner();
        lp.init(tenant, PLSEnvironments.pls_marketing_app_MKTO)
        PlsOperations.runModelingLoadGroups(tenant, PLSEnvironments.pls_marketing_app_MKTO);

        '''activate the inital model for the new tenant'''
        if False == lp.lpActivateModel(tenant):
            print "there is no new model been activated"
            assert False;

        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketoForDante(5);
        PlsOperations.runBulkScoring(tenant);
        time.sleep(15)
        lead_lists = mkto.getLeadFromMarketo(leads_list[0]);
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringMKTO",lead_lists);
        assert len(lead_faileds)==0, lead_faileds;

        danteLead = leads_list[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant,danteLead)

        leads_list = mkto.addLeadToMarketoForDante(2);
        PlsOperations.runHourlyScoring(tenant);
        time.sleep(15)
        lead_lists = mkto.getLeadFromMarketo(leads_list[0]);
        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringMKTO",lead_lists);
        assert len(lead_faileds)==0, lead_faileds;

        danteLead = leads_list[2].values()[0]
        dr.checkDanteValue(tenant,danteLead)

    def TestEndToEndSFDC(self):
        ''' prepare the Tenant -- drop templates, configure DL.. '''
        tenant = "AutoJekinsSFDC_%s_%s" % (time.strftime('%m_%d'), int(time.time()))
        lp = LPConfigRunner();
        lp.init(tenant, PLSEnvironments.pls_marketing_app_SFDC)
        PlsOperations.runModelingLoadGroups(tenant, PLSEnvironments.pls_marketing_app_SFDC);

        '''activate the inital model for the new tenant'''
        if False == lp.lpActivateModel(tenant):
            print "there is no new model been activated"
            assert False;

        sfdc = SFDCRequest();
        leads_list = sfdc.addLeadsToSFDC(5);
        contacts_list = sfdc.addContactsToSFDC(5)
        PlsOperations.runBulkScoring(tenant, PLSEnvironments.pls_marketing_app_SFDC);
        time.sleep(15)
        lead_lists = sfdc.getLeadsFromSFDC(leads_list);
        contact_lists = sfdc.getContactsFromSFDC(contacts_list);
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC", lead_lists);
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC", contact_lists);
        assert len(lead_faileds) == 0, lead_faileds;
        assert len(contact_faileds)==0, contact_faileds;

        danteLead = leads_list.keys()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant,danteLead)
        danteLead = contacts_list.keys()[0]
        dr.checkDanteValue(tenant,danteLead)

        leads_list = sfdc.addLeadsToSFDC(2);
        contacts_list = sfdc.addContactsToSFDC(2)
        PlsOperations.runHourlyScoring(tenant, PLSEnvironments.pls_marketing_app_SFDC);
        time.sleep(15)
        lead_lists = sfdc.getLeadsFromSFDC(leads_list);
        contact_lists = sfdc.getContactsFromSFDC(contacts_list);
        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringSFDC", lead_lists);
        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringSFDC", contact_lists);
        assert len(lead_faileds)==0, lead_faileds;
        assert len(contact_faileds) == 0, contact_faileds;

        danteLead = leads_list.keys()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant,danteLead)
        danteLead = contacts_list.keys()[0]
        dr.checkDanteValue(tenant,danteLead)
        
    def testName(self):
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()