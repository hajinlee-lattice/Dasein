package com.latticeengines.apps.cdl.end2end;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * Process Transaction imports after ProcessAccountDeploymentTestNG
 */
public class ProcessTransactionDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessTransactionDeploymentTestNG.class);

    static final String CHECK_POINT = "process2";

    private RatingEngine ratingEngine;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(ProcessAccountDeploymentTestNG.CHECK_POINT);

        new Thread(() -> {
            createTestSegment1();
            createTestSegment2();
        }).start();

        importData();
        processAnalyze();
        try {
            verifyProcess();
        } finally {
            saveCheckpoint(CHECK_POINT);
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
        verifyProcessAnalyzeReport(processAnalyzeAppId);
        verifyStats(false, BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.PurchaseHistory);

        long numAccounts = 500;
        long numContacts = 500;
        long numProducts = 99;
        long numTransactions = 1000;

//        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
//        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
//        Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()), numProducts);
//        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedRawTransaction), numTransactions);
//
//        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
//        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);

//        Map<BusinessEntity, Long> segment1Counts = ImmutableMap.of( //
//                BusinessEntity.Account, SEGMENT_1_ACCOUNT_1, BusinessEntity.Contact, SEGMENT_1_CONTACT_1);
//        verifyTestSegment1Counts(segment1Counts);
//        Map<BusinessEntity, Long> segment2Counts = ImmutableMap.of( //
//                BusinessEntity.Account, SEGMENT_2_ACCOUNT_1, BusinessEntity.Contact, SEGMENT_2_CONTACT_1);
//        verifyTestSegment2Counts(segment2Counts);
//        Map<RatingBucketName, Long> ratingCounts = ImmutableMap.of( //
//                RatingBucketName.A, RATING_A_COUNT_1, //
//                RatingBucketName.D, 7L, //
//                RatingBucketName.F, RATING_F_COUNT_1);
        // TODO: Rating engine needs to be activated
        // verifyRatingEngineCount(ratingEngine.getId(), ratingCounts);
    }

}
