package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.TRANSACTION_IMPORT_SIZE_1;

import java.util.Map;

import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

/**
 * Process Transaction imports after ProcessAccountDeploymentTestNG
 */
public class ProcessTransactionDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessTransactionDeploymentTestNG.class);

    static final String CHECK_POINT = "process2";

    private static final int TRANSACTION_IMPORT_SIZE_1_1 = 20000;
    private static final int TRANSACTION_IMPORT_SIZE_1_2 = 10000;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        Assert.assertEquals(TRANSACTION_IMPORT_SIZE_1_1 + TRANSACTION_IMPORT_SIZE_1_2, TRANSACTION_IMPORT_SIZE_1);
        resumeCheckpoint(ProcessAccountDeploymentTestNG.CHECK_POINT);
        importData();
        processAnalyze();
        verifyProcess();
        // saveCheckpoint(CHECK_POINT);
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockVdbImport(BusinessEntity.Transaction, 0, TRANSACTION_IMPORT_SIZE_1_1);
        Thread.sleep(2000);
        mockVdbImport(BusinessEntity.Transaction, TRANSACTION_IMPORT_SIZE_1_1, TRANSACTION_IMPORT_SIZE_1_2);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyProcess() {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(initialVersion.complement());

        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(statisticsContainer, "Should have statistics in active version");

        long numAccounts = countTableRole(BusinessEntity.Account.getBatchStore());
        Assert.assertEquals(numAccounts, ACCOUNT_IMPORT_SIZE_1);
        long numContacts = countTableRole(BusinessEntity.Contact.getBatchStore());
        Assert.assertEquals(numContacts, CONTACT_IMPORT_SIZE_1);
        long numProducts = countTableRole(BusinessEntity.Product.getBatchStore());
        Assert.assertEquals(numProducts, PRODUCT_IMPORT_SIZE_1);
        long numTransactions = countTableRole(TableRoleInCollection.ConsolidatedRawTransaction);
        Assert.assertEquals(numTransactions, TRANSACTION_IMPORT_SIZE_1);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), ACCOUNT_IMPORT_SIZE_1);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), CONTACT_IMPORT_SIZE_1);

        createTestSegment1();
        Map<BusinessEntity, Long> segment1Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_1_ACCOUNT_1,
                BusinessEntity.Contact, SEGMENT_1_CONTACT_1,
                BusinessEntity.Product, (long) PRODUCT_IMPORT_SIZE_1);
        verifyTestSegment1Counts(segment1Counts);
        createTestSegment2();
        Map<BusinessEntity, Long> segment2Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_2_ACCOUNT_1,
                BusinessEntity.Contact, SEGMENT_2_CONTACT_1,
                BusinessEntity.Product, (long) PRODUCT_IMPORT_SIZE_1);
        verifyTestSegment2Counts(segment2Counts);
        RatingEngine ratingEngine = createRuleBasedRatingEngine();
        Map<RuleBucketName, Long> ratingCounts = ImmutableMap.of( //
                RuleBucketName.A, RATING_A_COUNT_1, //
                RuleBucketName.D, RATING_D_COUNT_1, //
                RuleBucketName.F, RATING_F_COUNT_1
        );
        verifyRatingEngineCount(ratingEngine.getId(), ratingCounts);
    }

}
