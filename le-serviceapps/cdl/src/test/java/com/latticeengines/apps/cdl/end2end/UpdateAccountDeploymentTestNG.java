package com.latticeengines.apps.cdl.end2end;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;

public class UpdateAccountDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(UpdateAccountDeploymentTestNG.class);

    static final String CHECK_POINT = "update1";

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(resumeFromCheckPoint());
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), 500);

        new Thread(this::createTestSegment1).start();

        importData();
        processAnalyze();

        try {
            verifyProcess();
        } finally {
            if (isLocalEnvironment()) {
                saveCheckpoint(saveToCheckPoint());
            }
        }

    }

    protected void importData() throws Exception {
        mockCSVImport(BusinessEntity.Account, 3, "Account");
        Thread.sleep(2000);
    }

    protected void verifyProcess() {
        clearCache();
        runCommonPAVerifications();

        verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedReport());

        createTestSegment3();
        verifySegmentCountsNonNegative(SEGMENT_NAME_3, Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact));

        verifyBatchServingStoreCount();

        Map<BusinessEntity, Long> segment3Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_3_ACCOUNT_1, BusinessEntity.Contact, SEGMENT_3_CONTACT_1);
        verifyTestSegment3Counts(segment3Counts);
    }

    protected Map<BusinessEntity, Map<String, Object>> getExpectedReport() {
        Map<String, Object> accountReport = new HashMap<>();
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, ACCOUNT_2);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UNMATCH, 1L);
        accountReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        accountReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, ACCOUNT_3);

        Map<String, Object> purchaseHistoryReport = new HashMap<>();
        purchaseHistoryReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, 5L);

        Map<String, Object> contactReport = new HashMap<>();
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.UPDATE, 0L);
        contactReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        contactReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, CONTACT_1);

        Map<String, Object> productReport = new HashMap<>();
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_ID, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_HIERARCHY, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.PRODUCT_BUNDLE, 0L);
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.ERROR_MESSAGE, "");
        productReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.WARN_MESSAGE, "");

        Map<String, Object> transactionReport = new HashMap<>();
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW, 0L);
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        transactionReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, TRANSACTION_1);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Account, accountReport);
        expectedReport.put(BusinessEntity.Contact, contactReport);
        expectedReport.put(BusinessEntity.Product, productReport);
        expectedReport.put(BusinessEntity.Transaction, transactionReport);
        expectedReport.put(BusinessEntity.PurchaseHistory, purchaseHistoryReport);

        return expectedReport;
    }

    protected void verifyBatchServingStoreCount() {
        long numAccounts = 1000;
        long numContacts = 500;
        // long numProducts = 99;
        // long numTransactions = ???;
        //
        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
        // Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()),
        // numProducts);
        // Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedRawTransaction),
        // numTransactions);
        //
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);
    }

    protected String resumeFromCheckPoint() {
        return ProcessTransactionDeploymentTestNG.CHECK_POINT;
    }

    protected String saveToCheckPoint() {
        return CHECK_POINT;
    }

}
