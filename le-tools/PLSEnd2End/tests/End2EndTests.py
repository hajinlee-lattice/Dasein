'''
Created on Mar 18, 2015

@author: smeng
'''
import unittest
from Properties import PLSEnvironments;
from operations.LeadCreator import EloquaRequest;
from operations.LeadCreator import MarketoRequest;
from operations.LeadCreator import SFDCRequest;
from operations.TestHelpers import DanteRunner, DLConfigRunner, JamsRunner
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
        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_elq", tenant)
        PLSEnvironments.pls_tenant_elq = tenant

        PlsOperations.runModelingLoadGroups(tenant, PLSEnvironments.pls_marketing_app_ELQ);

        '''activate the inital model for the new tenant'''
        if False == lp.lpActivateModel(tenant):
            print "Activate model failed, please check if Model have been created"
            assert False;

        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_elq_model_activated", "True")
        PLSEnvironments.pls_tenant_elq_model_activated = "True"

        elq = EloquaRequest();
        contact_lists = elq.addEloquaContactForDante(2);
        PlsOperations.runBulkScoring(tenant);
        time.sleep(15)
        elq_contacts = elq.getEloquaContact(contact_lists[0]);
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringELQ",elq_contacts);
        assert len(contact_faileds)==0, contact_faileds;

        danteLead = contact_lists[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant, danteLead)

        contact_lists = elq.addEloquaContactForDante(1);
        PlsOperations.runHourlyScoring(tenant);
        time.sleep(15)
        elq_contacts = elq.getEloquaContact(contact_lists[0]);
        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringELQ",elq_contacts);
        assert len(contact_faileds)==0, contact_faileds;

        '''verify the dante result, check the leadcache table in dante database directly.'''
        # waitting to implement #dataplatforms
        danteLead = contact_lists[2].values()[0]
        dr.checkDanteValue(tenant, danteLead)

    def TestEndToEndMKTO(self):
        ''' prepare the Tenant -- drop templates, configure DL.. '''
        tenant = "AutoJekinsMKTO_%s_%s" % (time.strftime('%m_%d'), int(time.time()))
        lp = LPConfigRunner();
        lp.init(tenant, PLSEnvironments.pls_marketing_app_MKTO)
        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_mkto", tenant)
        PLSEnvironments.pls_tenant_mkto = tenant

        PlsOperations.runModelingLoadGroups(tenant, PLSEnvironments.pls_marketing_app_MKTO);

        '''activate the inital model for the new tenant'''
        if False == lp.lpActivateModel(tenant):
            print "Activate model failed, please check if Model have been created"
            assert False;

        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_mkto_model_activated", "True")
        PLSEnvironments.pls_tenant_mkto_model_activated = "True"

        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketoForDante(2);
        PlsOperations.runBulkScoring(tenant);
        time.sleep(15)
        lead_lists = mkto.getLeadFromMarketo(leads_list[0]);
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringMKTO", lead_lists);
        assert len(lead_faileds) == 0, lead_faileds;

        danteLead = leads_list[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant, danteLead)

        leads_list = mkto.addLeadToMarketoForDante(1);
        PlsOperations.runHourlyScoring(tenant);
        time.sleep(15)
        lead_lists = mkto.getLeadFromMarketo(leads_list[0]);
        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringMKTO", lead_lists);
        assert len(lead_faileds) == 0, lead_faileds;

        danteLead = leads_list[2].values()[0]
        dr.checkDanteValue(tenant, danteLead)

    def TestEndToEndSFDC(self):
        ''' prepare the Tenant -- drop templates, configure DL.. '''
        tenant = "AutoJekinsSFDC_%s_%s" % (time.strftime('%m_%d'), int(time.time()))
        lp = LPConfigRunner();
        lp.init(tenant, PLSEnvironments.pls_marketing_app_SFDC)
        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_sfdc", tenant)
        PLSEnvironments.pls_tenant_sfdc = tenant

        PlsOperations.runModelingLoadGroups(tenant, PLSEnvironments.pls_marketing_app_SFDC);

        '''activate the inital model for the new tenant'''
        if False == lp.lpActivateModel(tenant):
            print "Activate model failed, please check if Model have been created"
            assert False;
        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_sfdc_model_activated", "True")
        PLSEnvironments.pls_tenant_sfdc_model_activated = "True"

        sfdc = SFDCRequest();
        leads_list = sfdc.addLeadsToSFDC(2);
        contacts_list = sfdc.addContactsToSFDC(2)
        PlsOperations.runBulkScoring(tenant, PLSEnvironments.pls_marketing_app_SFDC);
        time.sleep(15)
        lead_lists = sfdc.getLeadsFromSFDC(leads_list);
        contact_lists = sfdc.getContactsFromSFDC(contacts_list);
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC", lead_lists);
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC", contact_lists);
        assert len(lead_faileds) == 0, lead_faileds;
        assert len(contact_faileds) == 0, contact_faileds;

        danteLead = leads_list.keys()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant, danteLead)
        danteLead = contacts_list.keys()[0]
        dr.checkDanteValue(tenant, danteLead)

        leads_list = sfdc.addLeadsToSFDC(1);
        contacts_list = sfdc.addContactsToSFDC(1)
        PlsOperations.runHourlyScoring(tenant, PLSEnvironments.pls_marketing_app_SFDC);
        time.sleep(15)
        lead_lists = sfdc.getLeadsFromSFDC(leads_list);
        contact_lists = sfdc.getContactsFromSFDC(contacts_list);
        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringSFDC", lead_lists);
        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringSFDC", contact_lists);
        assert len(lead_faileds) == 0, lead_faileds;
        assert len(contact_faileds) == 0, contact_faileds;

        danteLead = leads_list.keys()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant, danteLead)
        danteLead = contacts_list.keys()[0]
        dr.checkDanteValue(tenant, danteLead)

    def TestELQ_CreateNewTenant(self):
        # Cleanup config
        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_elq", "")
        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_elq_model_activated", "False")

        tenant = "AutoJekinsElq_%s_%s" % (time.strftime('%m_%d'), int(time.time()))

        lp = LPConfigRunner();
        created = lp.addNewTenant(tenant, "Eloqua", PLSEnvironments.jams_server, PLSEnvironments.pls_version,
                                  PLSEnvironments.dl_server_name)
        if created:
            PLSEnvironments.SetConfig("TestSetup", "pls_tenant_elq", tenant)
            PLSEnvironments.pls_tenant_elq = tenant
            assert lp.lpSFDCCredentials(tenant)
            assert lp.lpElQCredentials(tenant)
        else:
            print "Failed to add new tenant: %s via tenant console" % tenant
            assert False

    def TestELQ_ConfigDataLoader(self):
        tenant = PLSEnvironments.pls_tenant_elq
        if tenant == "":
            print "Please create or specify a tenant first"
            assert False
        dlConfig = DLConfigRunner();
        dlConfig.configDLTables(tenant, PLSEnvironments.pls_marketing_app_ELQ);
        dlConfig.createMockDataProviders(tenant, PLSEnvironments.pls_marketing_app_ELQ);
        dlConfig.editMockRefreshDataSources(tenant, PLSEnvironments.pls_marketing_app_ELQ);
        dlConfig.loadCfgTables(tenant);
        jamsRunner = JamsRunner()
        jamsRunner.setJamsTenant(tenant)

    def TestELQ_Modeling(self):
        tenant = PLSEnvironments.pls_tenant_elq
        if tenant == "":
            print "Please create or specify a tenant first"
            assert False

        PlsOperations.runModelingLoadGroups(tenant, PLSEnvironments.pls_marketing_app_ELQ);

        '''activate the inital model for the new tenant'''
        lp = LPConfigRunner()
        if False == lp.lpActivateModel(tenant):
            print "Activate model failed, please check if Model have been created"
            assert False

        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_elq_model_activated", "True")
        PLSEnvironments.pls_tenant_elq_model_activated = "True"

    def TestELQ_BulkScoring(self):
        tenant = PLSEnvironments.pls_tenant_elq
        if tenant == "":
            print "Please create or specify a tenant first"
            assert False

        if PLSEnvironments.pls_tenant_elq_model_activated == "False":
            print "Please create a Model and Activate first"
            assert False

        elq = EloquaRequest()
        contact_lists = elq.addEloquaContactForDante(2)
        PlsOperations.runBulkScoring(tenant)
        time.sleep(15)
        print contact_lists[0]
        contact_lists = elq.getEloquaContact(contact_lists[0])
        print contact_lists
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringELQ", contact_lists)
        assert len(contact_faileds) == 0, contact_faileds

        danteLead = contact_lists[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant, danteLead)

    def TestELQ_HourlyScoring(self):
        tenant = PLSEnvironments.pls_tenant_elq
        if tenant == "":
            print "Please create or specify a tenant first"
            assert False

        if PLSEnvironments.pls_tenant_elq_model_activated == "False":
            print "Please create a Model and Activate first"
            assert False

        elq = EloquaRequest();
        contact_lists = elq.addEloquaContactForDante(1);
        PlsOperations.runHourlyScoring(tenant);
        time.sleep(15)
        print contact_lists[0]
        contact_lists = elq.getEloquaContact(contact_lists[0])
        print contact_lists
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringELQ", contact_lists)
        assert len(contact_faileds) == 0, contact_faileds

        danteLead = contact_lists[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant, danteLead)

    def TestMKTO_CreateNewTenant(self):
        # Cleanup config
        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_mkto", "")
        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_mkto_model_activated", "False")

        tenant = "AutoJekinsMKTO_%s_%s" % (time.strftime('%m_%d'), int(time.time()))

        lp = LPConfigRunner();
        created = lp.addNewTenant(tenant, "Marketo", PLSEnvironments.jams_server, PLSEnvironments.pls_version,
                                  PLSEnvironments.dl_server_name)
        if created:
            PLSEnvironments.SetConfig("TestSetup", "pls_tenant_mkto", tenant)
            PLSEnvironments.pls_tenant_mkto = tenant
            assert lp.lpSFDCCredentials(tenant)
            assert lp.lpElQCredentials(tenant)
        else:
            print "Failed to add new tenant: %s via tenant console" % tenant
            assert False

    def TestMKTO_ConfigDataLoader(self):
        tenant = PLSEnvironments.pls_tenant_mkto
        if tenant == "":
            print "Please create or specify a tenant first"
            assert False
        dlConfig = DLConfigRunner();
        dlConfig.configDLTables(tenant, PLSEnvironments.pls_marketing_app_MKTO);
        dlConfig.createMockDataProviders(tenant, PLSEnvironments.pls_marketing_app_MKTO);
        dlConfig.editMockRefreshDataSources(tenant, PLSEnvironments.pls_marketing_app_MKTO);
        dlConfig.loadCfgTables(tenant);
        jamsRunner = JamsRunner()
        jamsRunner.setJamsTenant(tenant)

    def TestMKTO_Modeling(self):
        tenant = PLSEnvironments.pls_tenant_mkto
        if tenant == "":
            print "Please create or specify a tenant first"
            assert False

        PlsOperations.runModelingLoadGroups(tenant, PLSEnvironments.pls_marketing_app_MKTO);

        '''activate the inital model for the new tenant'''
        lp = LPConfigRunner()
        if False == lp.lpActivateModel(tenant):
            print "Activate model failed, please check if Model have been created"
            assert False

        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_mkto_model_activated", "True")
        PLSEnvironments.pls_tenant_mkto_model_activated = "True"

    def TestMKTO_BulkScoring(self):
        tenant = PLSEnvironments.pls_tenant_mkto
        if tenant == "":
            print "Please create or specify a tenant first"
            assert False

        if PLSEnvironments.pls_tenant_mkto_model_activated == "False":
            print "Please create a Model and Activate first"
            assert False

        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketoForDante(2);
        PlsOperations.runBulkScoring(tenant);
        time.sleep(15)
        print leads_list
        lead_lists = mkto.getLeadFromMarketo(leads_list[0]);
        print lead_lists
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringMKTO", lead_lists);
        assert len(lead_faileds) == 0, lead_faileds;

        danteLead = leads_list[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant, danteLead)

    def TestMKTO_HourlyScoring(self):
        tenant = PLSEnvironments.pls_tenant_mkto
        if tenant == "":
            print "Please create or specify a tenant first"
            assert False

        if PLSEnvironments.pls_tenant_mkto_model_activated == "False":
            print "Please create a Model and Activate first"
            assert False

        mkto = MarketoRequest();
        leads_list = mkto.addLeadToMarketoForDante(1);
        PlsOperations.runHourlyScoring(tenant)
        time.sleep(15)
        print leads_list
        lead_lists = mkto.getLeadFromMarketo(leads_list[0]);
        print lead_lists
        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringMKTO", lead_lists)
        assert len(lead_faileds) == 0, lead_faileds;

        danteLead = leads_list[2].values()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant, danteLead)

    def TestSFDC_CreateNewTenant(self):
        # Cleanup config
        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_sfdc", "")
        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_sfdc_model_activated", "False")

        tenant = "AutoJekinsSFDC_%s_%s" % (time.strftime('%m_%d'), int(time.time()))

        lp = LPConfigRunner();
        created = lp.addNewTenant(tenant, "SFDC", PLSEnvironments.jams_server, PLSEnvironments.pls_version,
                                  PLSEnvironments.dl_server_name)
        if created:
            PLSEnvironments.SetConfig("TestSetup", "pls_tenant_sfdc", tenant)
            PLSEnvironments.pls_tenant_sfdc = tenant
            assert lp.lpSFDCCredentials(tenant)
        else:
            print "Failed to add new tenant: %s via tenant console" % tenant
            assert False

    def TestSFDC_ConfigDataLoader(self):
        tenant = PLSEnvironments.pls_tenant_sfdc
        if tenant == "":
            print "Please create or specify a tenant first"
            assert False
        dlConfig = DLConfigRunner();
        dlConfig.configDLTables(tenant, PLSEnvironments.pls_marketing_app_SFDC);
        dlConfig.createMockDataProviders(tenant, PLSEnvironments.pls_marketing_app_SFDC);
        dlConfig.editMockRefreshDataSources(tenant, PLSEnvironments.pls_marketing_app_SFDC);
        dlConfig.loadCfgTables(tenant);
        jamsRunner = JamsRunner()
        jamsRunner.setJamsTenant(tenant)

    def TestSFDC_Modeling(self):
        tenant = PLSEnvironments.pls_tenant_sfdc
        if tenant == "":
            print "Please create or specify a tenant first"
            assert False

        PlsOperations.runModelingLoadGroups(tenant, PLSEnvironments.pls_marketing_app_SFDC);

        '''activate the inital model for the new tenant'''
        lp = LPConfigRunner()
        if False == lp.lpActivateModel(tenant):
            print "Activate model failed, please check if Model have been created"
            assert False

        PLSEnvironments.SetConfig("TestSetup", "pls_tenant_sfdc_model_activated", "True")
        PLSEnvironments.pls_tenant_elq_model_activated = "True"

    def TestSFDC_BulkScoring(self):
        tenant = PLSEnvironments.pls_tenant_sfdc
        if tenant == "":
            print "Please create or specify a tenant first"
            assert False

        if PLSEnvironments.pls_tenant_sfdc_model_Activated == "False":
            print "Please create a Model and Activate first"
            assert False

        sfdc = SFDCRequest()
        leads_list = sfdc.addLeadsToSFDC(2)
        contacts_list = sfdc.addContactsToSFDC(2)
        PlsOperations.runBulkScoring(tenant, PLSEnvironments.pls_marketing_app_SFDC);
        time.sleep(15)
        lead_lists = sfdc.getLeadsFromSFDC(leads_list)
        contact_lists = sfdc.getContactsFromSFDC(contacts_list)
        lead_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC", lead_lists)
        contact_faileds = LeadCreator.verifyResult("TestBulkScoringSFDC", contact_lists)
        assert len(lead_faileds) == 0, lead_faileds;
        assert len(contact_faileds) == 0, contact_faileds;

        danteLead = leads_list.keys()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant, danteLead)
        danteLead = contacts_list.keys()[0]
        dr.checkDanteValue(tenant, danteLead)

    def TestSFDC_HourlyScoring(self):
        tenant = PLSEnvironments.pls_tenant_sfdc
        if tenant == "":
            print "Please create or specify a tenant first"
            assert False

        if PLSEnvironments.pls_tenant_sfdc_model_Activated == "False":
            print "Please create a Model and Activate first"
            assert False

        sfdc = SFDCRequest()
        leads_list = sfdc.addLeadsToSFDC(1)
        contacts_list = sfdc.addContactsToSFDC(1)
        PlsOperations.runHourlyScoring(tenant, PLSEnvironments.pls_marketing_app_SFDC)
        time.sleep(15)
        lead_lists = sfdc.getLeadsFromSFDC(leads_list)
        contact_lists = sfdc.getContactsFromSFDC(contacts_list)
        lead_faileds = LeadCreator.verifyResult("TestHourlyScoringSFDC", lead_lists)
        contact_faileds = LeadCreator.verifyResult("TestHourlyScoringSFDC", contact_lists)
        assert len(lead_faileds) == 0, lead_faileds;
        assert len(contact_faileds) == 0, contact_faileds;

        danteLead = leads_list.keys()[0]
        dr = DanteRunner()
        dr.checkDanteValue(tenant, danteLead)
        danteLead = contacts_list.keys()[0]
        dr.checkDanteValue(tenant, danteLead)

    def testName(self):
        pass


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
