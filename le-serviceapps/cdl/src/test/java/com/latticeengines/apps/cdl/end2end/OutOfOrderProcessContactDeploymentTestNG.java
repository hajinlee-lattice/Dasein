package com.latticeengines.apps.cdl.end2end;

import java.util.HashMap;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;


public class OutOfOrderProcessContactDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final String UNDER_SCORE = "_";

    @Test(groups = "deployment")
    public void runTest() throws Exception {
        importData();
        processAnalyze();
        verifyProcess();
    }

    protected void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockCSVImport(BusinessEntity.Contact, 1, "DefaultSystem_ContactData");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Contact, 2, "DefaultSystem_ContactData");
        Thread.sleep(2000);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    protected void verifyProcess() {
        clearCache();
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(DataCollection.Version.Green);
        verifyDataCollectionStatus(DataCollection.Version.Green);
        verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedReport());
        verifyBatchStore(getExpectedBatchStoreCounts());
    }

    @Override
    void verifyDataCollectionStatus(DataCollection.Version version) {
        DataCollectionStatus dataCollectionStatus = dataCollectionProxy
                .getOrCreateDataCollectionStatus(mainTestTenant.getId(), version);
        Assert.assertNotNull(dataCollectionStatus);
    }

    private Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.NEW, CONTACT_PA);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + UNDER_SCORE + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + UNDER_SCORE + ReportConstants.TOTAL, CONTACT_PA);
        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Contact, contactReport);
        return expectedReport;
    }

    private Map<BusinessEntity, Long> getExpectedBatchStoreCounts() {
        return ImmutableMap.of(BusinessEntity.Contact, CONTACT_PA);
    }

}
