package com.latticeengines.apps.cdl.end2end;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

public class UpdateAccountWithAdvancedMatchDeploymentTestNG extends UpdateAccountDeploymentTestNG {

    private static final Logger log = LoggerFactory.getLogger(UpdateAccountWithAdvancedMatchDeploymentTestNG.class);

    static final String CHECK_POINT = "entitymatch_update1";

    @BeforeClass(groups = { "end2end" })
    @Override
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        log.info("Setup Complete!");
    }

    @Test(groups = "end2end", enabled = true)
    @Override
    public void runTest() throws Exception {
        super.runTest();
    }

    @Override
    protected void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockCSVImport(BusinessEntity.Account, 3, "Account");
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    @Override
    protected String getAvroFileVersion() {
        // advanced matching should use a different version
        return S3_AVRO_VERSION_ADVANCED_MATCH;
    }

    @Override
    protected void verifyProcess() {
        super.verifyProcess();
    }

    // TODO: After adding contact import, need to change expected numContacts
    @Override
    protected void verifyBatchServingStoreCount() {
        long numAccounts = 1000;
        long numContacts = 500;

        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);
    }

    // TODO: After adding contact import, need to change expected
    // new/update/total #contact in report
    @Override
    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, ACCOUNT_2);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UNMATCH, 1L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, ACCOUNT_3);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_1);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, accountReport);
        expectedReport.put(BusinessEntity.Contact, contactReport);

        return expectedReport;
    }

    @Override
    protected String resumeFromCheckPoint() {
        return ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT;
    }

    @Override
    protected String saveToCheckPoint() {
        return CHECK_POINT;
    }

}
