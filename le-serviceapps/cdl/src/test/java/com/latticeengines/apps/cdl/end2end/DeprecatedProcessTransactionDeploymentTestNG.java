package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.apps.cdl.end2end.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.CheckpointService.PRODUCT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.CheckpointService.TRANSACTION_IMPORT_SIZE_1;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * Process Transaction imports after ProcessAccountDeploymentTestNG
 */
public class DeprecatedProcessTransactionDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DeprecatedProcessTransactionDeploymentTestNG.class);

    static final String CHECK_POINT = "process2";

    private static final int TRANSACTION_IMPORT_SIZE_1_1 = 20000;
    private static final int TRANSACTION_IMPORT_SIZE_1_2 = 10000;
    private static final long AGGREGATE_TRANSACTION_SIZE = 22747L;
    private static final long AGGREGATE_PERIOD_TRANSACTION_SIZE = 77266L;
    private static final long DEPIVOTED_METRICS_SIZE = 7871L;

    private RatingEngine ratingEngine;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        Assert.assertEquals(TRANSACTION_IMPORT_SIZE_1_1 + TRANSACTION_IMPORT_SIZE_1_2, TRANSACTION_IMPORT_SIZE_1);
        resumeCheckpoint(DeprecatedProcessAccountDeploymentTestNG.CHECK_POINT);

        new Thread(() -> {
            createTestSegment1();
            createTestSegment2();
            ratingEngine = createRuleBasedRatingEngine();
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
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockVdbImport(BusinessEntity.Transaction, 0, TRANSACTION_IMPORT_SIZE_1_1);
        Thread.sleep(2000);
        mockVdbImport(BusinessEntity.Transaction, TRANSACTION_IMPORT_SIZE_1_1, TRANSACTION_IMPORT_SIZE_1_2);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyProcess() {
        runCommonPAVerifications();

        verifyProcessAnalyzeReport(processAnalyzeAppId);

        verifyStats(BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.PurchaseHistory);

        long numAccounts = ACCOUNT_IMPORT_SIZE_1;
        long numContacts = CONTACT_IMPORT_SIZE_1;
        long numProducts = PRODUCT_IMPORT_SIZE_1;
        long numTransactions = TRANSACTION_IMPORT_SIZE_1;

        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
        Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()), numProducts);
        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedRawTransaction), numTransactions);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);

        Map<BusinessEntity, Long> segment1Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_1_ACCOUNT_1, BusinessEntity.Contact, SEGMENT_1_CONTACT_1);
        verifyTestSegment1Counts(segment1Counts);
        Map<BusinessEntity, Long> segment2Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_2_ACCOUNT_1, BusinessEntity.Contact, SEGMENT_2_CONTACT_1);
        verifyTestSegment2Counts(segment2Counts);
        Map<RatingBucketName, Long> ratingCounts = ImmutableMap.of( //
                RatingBucketName.A, RATING_A_COUNT_1, //
                RatingBucketName.D, 7L, //
                RatingBucketName.F, RATING_F_COUNT_1);
        // TODO: Rating engine needs to be activated
        // verifyRatingEngineCount(ratingEngine.getId(), ratingCounts);
    }

}
