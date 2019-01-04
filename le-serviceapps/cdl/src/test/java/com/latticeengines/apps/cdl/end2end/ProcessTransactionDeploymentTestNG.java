package com.latticeengines.apps.cdl.end2end;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;

/**
 * Process Transaction imports after ProcessAccountDeploymentTestNG
 */
public class ProcessTransactionDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessTransactionDeploymentTestNG.class);

    public static final String CHECK_POINT = "process2";

    @Inject
    private EntityProxy entityProxy;

    private RatingEngine ratingEngine;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(ProcessAccountDeploymentTestNG.CHECK_POINT);
        verifyNumAttrsInAccount();
        verifyDateTypeAttrs();
        new Thread(() -> {
            createTestSegment1();
            createTestSegment2();
        }).start();

        importData();
        processAnalyze();
        try {
            verifyProcess();
        } finally {
            if (isLocalEnvironment()) {
                saveCheckpoint(CHECK_POINT);
            }
        }
    }

    private void importData() throws Exception {
        mockCSVImport(BusinessEntity.Transaction, 1, "Transaction");
        Thread.sleep(2000);
        mockCSVImport(BusinessEntity.Transaction, 2, "Transaction");
        Thread.sleep(2000);
    }

    private void verifyProcess() {
        runCommonPAVerifications();

        Map<String, Object> transactionReport = new HashMap<>();
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.NEW,
                TRANSACTION_1);
        transactionReport.put(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.name() + "_" + ReportConstants.DELETE, 0L);
        transactionReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL, TRANSACTION_1);

        Map<String, Object> purchaseHistoryReport = new HashMap<>();
        purchaseHistoryReport.put(ReportPurpose.ENTITY_STATS_SUMMARY.name() + "_" + ReportConstants.TOTAL,
                PURCHASE_HISTORY_1);

        Map<BusinessEntity, Map<String, Object>> expectedReport = new HashMap<>();
        expectedReport.put(BusinessEntity.Transaction, transactionReport);
        expectedReport.put(BusinessEntity.PurchaseHistory, purchaseHistoryReport);

        verifyProcessAnalyzeReport(processAnalyzeAppId, expectedReport);
        verifyNumAttrsInAccount();
        verifyStats(false, BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.PurchaseHistory);

        long numAccounts = 500;
        long numContacts = 500;
        long numProducts = 99;
        long numTransactions = 1000;

        // Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()),
        // numAccounts);
        // Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()),
        // numContacts);
        // Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()),
        // numProducts);
        // Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedRawTransaction),
        // numTransactions);
        //
        // Assert.assertEquals(countInRedshift(BusinessEntity.Account),
        // numAccounts);
        // Assert.assertEquals(countInRedshift(BusinessEntity.Contact),
        // numContacts);

        // Map<BusinessEntity, Long> segment1Counts = ImmutableMap.of( //
        // BusinessEntity.Account, SEGMENT_1_ACCOUNT_1, BusinessEntity.Contact,
        // SEGMENT_1_CONTACT_1);
        // verifyTestSegment1Counts(segment1Counts);
        // Map<BusinessEntity, Long> segment2Counts = ImmutableMap.of( //
        // BusinessEntity.Account, SEGMENT_2_ACCOUNT_1, BusinessEntity.Contact,
        // SEGMENT_2_CONTACT_1);
        // verifyTestSegment2Counts(segment2Counts);
    }

    private void verifyNumAttrsInAccount() {
        String tableName = dataCollectionProxy.getTableName(mainCustomerSpace,
                BusinessEntity.Account.getServingStore());
        List<ColumnMetadata> cms = metadataProxy.getTableColumns(mainCustomerSpace, tableName);
        Assert.assertTrue(cms.size() < 20000, "Should not have more than 20000 account attributes");
    }

    private void verifyDateTypeAttrs() {
        FrontEndQuery query = new FrontEndQuery();
        query.setMainEntity(BusinessEntity.Account);
        Bucket bkt = Bucket.dateBkt(TimeFilter.ever());
        Restriction restriction = new BucketRestriction(BusinessEntity.Account, "user_Test_Date", bkt);
        query.setAccountRestriction(new FrontEndRestriction(restriction));
        Long count = entityProxy.getCount(mainCustomerSpace, query);
        Assert.assertEquals(count, ACCOUNT_1);

        bkt = Bucket.dateBkt(TimeFilter.isEmpty());
        restriction = new BucketRestriction(BusinessEntity.Account, "user_Test_Date", bkt);
        query.setAccountRestriction(new FrontEndRestriction(restriction));
        count = entityProxy.getCount(mainCustomerSpace, query);
        Assert.assertEquals(count.longValue(), 0);
        log.info("verify date done");
    }

}
