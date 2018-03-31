package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_TOTAL;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_OVERLAP;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_TOTAL;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.TRANSACTION_IMPORT_SIZE_1;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class UpdateContactDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(UpdateContactDeploymentTestNG.class);

    static final String CHECK_POINT = "update2";

    private RatingEngine ratingEngine;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeVdbCheckpoint(UpdateAccountDeploymentTestNG.CHECK_POINT);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), CONTACT_IMPORT_SIZE_1);

        // For the test scenario of non-1st profile purchase history
        dataFeedProxy.updateEarliestLatestTransaction(mainTestTenant.getId(), EARLIEST_TRANSACTION, LATEST_TRANSACTION);

        new Thread(() -> {
            createTestSegments();
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

    @Override
    protected void updateDataCloudBuildNumber() {
    }

    private void importData() throws Exception {
        mockVdbImport(BusinessEntity.Contact, CONTACT_IMPORT_SIZE_1 - CONTACT_IMPORT_SIZE_OVERLAP,
                CONTACT_IMPORT_SIZE_2);
        Thread.sleep(2000);
    }

    private void verifyProcess() {
        runCommonPAVerifications();

        verifyProcessAnalyzeReport(processAnalyzeAppId, getExpectedCnts());

        long numAccounts = ACCOUNT_IMPORT_SIZE_TOTAL;
        long numContacts = CONTACT_IMPORT_SIZE_TOTAL;
        long numProducts = PRODUCT_IMPORT_SIZE_1;
        long numTransactions = TRANSACTION_IMPORT_SIZE_1;

        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
        Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()), numProducts);
        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedRawTransaction), numTransactions);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);

        Map<BusinessEntity, Long> segment1Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_1_ACCOUNT_3, BusinessEntity.Contact, SEGMENT_1_CONTACT_3);
        verifyTestSegment1Counts(segment1Counts);
        Map<BusinessEntity, Long> segment2Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_2_ACCOUNT_2_REBUILD, BusinessEntity.Contact,
                SEGMENT_2_CONTACT_2_REBUILD);
        verifyTestSegment2Counts(segment2Counts);
        Map<RatingBucketName, Long> ratingCounts = ImmutableMap.of( //
                RatingBucketName.A, RATING_A_COUNT_2_REBUILD, //
                RatingBucketName.D, RATING_D_COUNT_2_REBUILD, //
                RatingBucketName.F, RATING_F_COUNT_2_REBUILD);
        verifyRatingEngineCount(ratingEngine.getId(), ratingCounts);
    }

    private Map<TableRoleInCollection, Long> getExpectedCnts() {
        Map<TableRoleInCollection, Long> expectedCnts = new HashMap<>();
        expectedCnts.put(TableRoleInCollection.SortedContact, (long) CONTACT_IMPORT_SIZE_2);
        // Because Account is enforced to rebuild
        expectedCnts.put(TableRoleInCollection.BucketedAccount, (long) ACCOUNT_IMPORT_SIZE_TOTAL);
        expectedCnts.put(TableRoleInCollection.CalculatedPurchaseHistory, (long) ACCOUNT_IMPORT_SIZE_TOTAL);
        expectedCnts.put(TableRoleInCollection.CalculatedDepivotedPurchaseHistory,
                (long) (ACCOUNT_IMPORT_SIZE_TOTAL * PRODUCT_IMPORT_SIZE_1));
        return expectedCnts;
    }

}
