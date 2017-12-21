package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.TRANSACTION_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.TRANSACTION_IMPORT_SIZE_2;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessAllDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessAllDeploymentTestNG.class);

    static final String CHECK_POINT = "process";

    @Test(groups = "end2end")
    public void runTest() throws Exception {
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
        mockVdbImport(BusinessEntity.Account, 0, ACCOUNT_IMPORT_SIZE_1);
        mockVdbImport(BusinessEntity.Contact, 0, CONTACT_IMPORT_SIZE_1);
        mockVdbImport(BusinessEntity.Product, 0, PRODUCT_IMPORT_SIZE_1);
        mockVdbImport(BusinessEntity.Transaction, 0, TRANSACTION_IMPORT_SIZE_1);
        Thread.sleep(2000);
        mockVdbImport(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1, ACCOUNT_IMPORT_SIZE_2);
        mockVdbImport(BusinessEntity.Contact, CONTACT_IMPORT_SIZE_1, CONTACT_IMPORT_SIZE_2);
        mockVdbImport(BusinessEntity.Product, PRODUCT_IMPORT_SIZE_1, PRODUCT_IMPORT_SIZE_2);
        mockVdbImport(BusinessEntity.Transaction, TRANSACTION_IMPORT_SIZE_1, TRANSACTION_IMPORT_SIZE_2);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyProcess() {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(DataCollection.Version.Green);

        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(statisticsContainer, "Should have statistics in active version");

        long numAccounts = ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2;
        long numContacts = CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2;
        long numProducts = PRODUCT_IMPORT_SIZE_1 + PRODUCT_IMPORT_SIZE_2;
        long numTransactions = TRANSACTION_IMPORT_SIZE_1 + TRANSACTION_IMPORT_SIZE_2;

        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
        Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()), numProducts);
        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedRawTransaction), numTransactions);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);

        createTestSegments();
        Map<BusinessEntity, Long> segment1Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_1_ACCOUNT_4,
                BusinessEntity.Contact, SEGMENT_1_CONTACT_4,
                BusinessEntity.Product, numProducts);
        verifyTestSegment1Counts(segment1Counts);
        Map<BusinessEntity, Long> segment2Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_2_ACCOUNT_2,
                BusinessEntity.Contact, SEGMENT_2_CONTACT_2,
                BusinessEntity.Product, numProducts);
        verifyTestSegment2Counts(segment2Counts);

        RatingEngine ratingEngine = createRuleBasedRatingEngine();
        Map<RuleBucketName, Long> ratingCounts = ImmutableMap.of( //
                RuleBucketName.A, RATING_A_COUNT_2, //
                RuleBucketName.D, RATING_D_COUNT_2, //
                RuleBucketName.F, RATING_F_COUNT_2
        );
        verifyRatingEngineCount(ratingEngine.getId(), ratingCounts);
    }

}
