package com.latticeengines.apps.cdl.end2end;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

public class ProcessAccountWithEraseByNullDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ProcessAccountWithEraseByNullDeploymentTestNG.class);

    static final String UNDER_SCORE = "_";

    static final String ERASE_PREFIX = "Erase_";

    @BeforeClass(groups = "end2end")
    @Override
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_IMPORT_ERASE_BY_NULL enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_IMPORT_ERASE_BY_NULL.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        log.info("Setup Complete!");
    }

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(ProcessAccountDeploymentTestNG.CHECK_POINT);
        importData();
        processAnalyze(JobStatus.COMPLETED);
        verifyProcess();
    }

    protected void importData() {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        importData(BusinessEntity.Account, "Account_1_10_erase_null.csv");
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    protected void processAnalyze(JobStatus result) {
        log.info("Start processing and analyzing ...");
        ApplicationId appId = cdlProxy.processAnalyze(mainTestTenant.getId(), null);
        processAnalyzeAppId = appId.toString();
        log.info("processAnalyzeAppId=" + processAnalyzeAppId);
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(status, result);
    }

    protected void verifyProcess() {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyUpdateActions();
        verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedReport());
        verifyBatchStore(getExpectedBatchStoreCounts());
        verifyServingStore(getExpectedServingStoreCounts());
        Table batchStoreTable = dataCollectionProxy.getTable(mainCustomerSpace, BusinessEntity.Account.getBatchStore());
        Table servingStoreTable = dataCollectionProxy.getTable(mainCustomerSpace,
                BusinessEntity.Account.getServingStore());
        verifyAttribute(batchStoreTable);
        verifyAttribute(servingStoreTable);
    }

    private void verifyAttribute(Table table) {
        Assert.assertNotNull(table);
        Assert.assertNotNull(table.getAttributes());
        for (Attribute attr : table.getAttributes()) {
            Assert.assertFalse(attr.getName().contains(ERASE_PREFIX));
        }
    }

    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.NEW, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UPDATE, 10L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UNMATCH, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + UNDER_SCORE + ReportConstants.TOTAL, ACCOUNT_PA);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_PA);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, accountReport);
        expectedReport.put(BusinessEntity.Contact, contactReport);

        return expectedReport;
    }

    protected Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        return ImmutableMap.of( //
                BusinessEntity.Account, ACCOUNT_PA, //
                BusinessEntity.Contact, CONTACT_PA //
        );
    }

    protected Map<BusinessEntity, Long> getExpectedServingStoreCounts() {
        Map<BusinessEntity, Long> map = new HashMap<>();
        map.put(BusinessEntity.Account, ACCOUNT_PA);
        map.put(BusinessEntity.Contact, CONTACT_PA);
        return map;
    }
}
