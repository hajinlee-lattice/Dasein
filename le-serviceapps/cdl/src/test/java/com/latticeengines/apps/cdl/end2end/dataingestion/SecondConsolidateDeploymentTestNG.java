package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.TRANSACTION_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.TRANSACTION_IMPORT_SIZE_2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class SecondConsolidateDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SecondConsolidateDeploymentTestNG.class);

    private StatisticsContainer preConsolidateStats;
    private RatingEngine ratingEngine;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint("profile1");
        verifyFirstProfileCheckpoint();

        uploadRedshift();

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), ACCOUNT_IMPORT_SIZE_1);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), CONTACT_IMPORT_SIZE_1);

        createTestSegments();
        Map<BusinessEntity, Long> segment1Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_1_ACCOUNT_1,
                BusinessEntity.Contact, SEGMENT_1_CONTACT_1,
                BusinessEntity.Product, (long) PRODUCT_IMPORT_SIZE_1);
        verifyTestSegment1Counts(segment1Counts);
        Map<BusinessEntity, Long> segment2Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_2_ACCOUNT_1,
                BusinessEntity.Contact, SEGMENT_2_CONTACT_1,
                BusinessEntity.Product, (long) PRODUCT_IMPORT_SIZE_1);
        verifyTestSegment2Counts(segment2Counts);

        ratingEngine = createRuleBasedRatingEngine();
        Map<RuleBucketName, Long> ratingCounts = ImmutableMap.of( //
                RuleBucketName.A, RATING_A_COUNT_1, //
                RuleBucketName.D, RATING_D_COUNT_1, //
                RuleBucketName.F, RATING_F_COUNT_1
        );
        verifyRatingEngineCount(ratingEngine.getId(), ratingCounts);

        preConsolidateStats = dataCollectionProxy.getStats(mainTestTenant.getId());

        importData();
        consolidate();
        verifyConsolidate();

        verifySecondConsolidateCheckpoint();
        saveCheckpoint("consolidate2");
    }

    private void uploadRedshift() {
        log.info("Exporting checkpoint data to redshift. This may take more than 20 min ...");
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        List<Future<Boolean>> futures = new ArrayList<>();
        futures.add(executorService.submit(() -> exportEntityToRedshift(BusinessEntity.Account)));
        futures.add(executorService.submit(() -> exportEntityToRedshift(BusinessEntity.Contact)));
        futures.add(executorService.submit(() -> exportEntityToRedshift(BusinessEntity.Transaction)));
        futures.add(executorService.submit(() -> exportEntityToRedshift(BusinessEntity.Product)));
        while (!futures.isEmpty()) {
            List<Future<Boolean>> toDelete = new ArrayList<>();
            futures.forEach(future -> {
                try {
                    future.get(5, TimeUnit.SECONDS);
                    toDelete.add(future);
                } catch (TimeoutException e) {
                    // ignore
                } catch (Exception e) {
                    throw new RuntimeException("Failed to upload to redshift.", e);
                }
            });
            futures.removeAll(toDelete);
        }
        executorService.shutdown();
    }

    private void importData() throws Exception {
        mockVdbImport(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1, ACCOUNT_IMPORT_SIZE_2);
        mockVdbImport(BusinessEntity.Contact, CONTACT_IMPORT_SIZE_1, CONTACT_IMPORT_SIZE_2);
        mockVdbImport(BusinessEntity.Product, PRODUCT_IMPORT_SIZE_1, PRODUCT_IMPORT_SIZE_2);
        mockVdbImport(BusinessEntity.Transaction, TRANSACTION_IMPORT_SIZE_1, TRANSACTION_IMPORT_SIZE_2);
        Thread.sleep(2000);
    }

    private void verifyConsolidate() {
        Map<TableRoleInCollection, Long> expectedCounts = ImmutableMap.of( //
                BusinessEntity.Account.getServingStore(), (long) ACCOUNT_IMPORT_SIZE_2, //
                BusinessEntity.Contact.getServingStore(), (long) CONTACT_IMPORT_SIZE_2, //
                BusinessEntity.Product.getServingStore(), (long) PRODUCT_IMPORT_SIZE_2, //
                BusinessEntity.Transaction.getServingStore(),
                (long) TRANSACTION_IMPORT_SIZE_1 + TRANSACTION_IMPORT_SIZE_2);
        verifyConsolidateReport(consolidateAppId, expectedCounts);
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(initialVersion);

        StatisticsContainer postConsolidateStats = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertEquals(preConsolidateStats.getName(), postConsolidateStats.getName());

        long numAccounts = ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2;
        long numContacts = CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2;
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);

        Map<BusinessEntity, Long> segment1Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_1_ACCOUNT_2,
                BusinessEntity.Contact, SEGMENT_1_CONTACT_2,
                BusinessEntity.Product, (long) PRODUCT_IMPORT_SIZE_2);
        verifyTestSegment1Counts(segment1Counts);
        Map<BusinessEntity, Long> segment2Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_2_ACCOUNT_2,
                BusinessEntity.Contact, SEGMENT_2_CONTACT_2,
                BusinessEntity.Product, (long) PRODUCT_IMPORT_SIZE_2);
        verifyTestSegment2Counts(segment2Counts);
        Map<RuleBucketName, Long> ratingCounts = ImmutableMap.of( //
                RuleBucketName.A, RATING_A_COUNT_2, //
                RuleBucketName.D, RATING_D_COUNT_2, //
                RuleBucketName.F, RATING_F_COUNT_2
        );
        verifyRatingEngineCount(ratingEngine.getId(), ratingCounts);

    }

}
