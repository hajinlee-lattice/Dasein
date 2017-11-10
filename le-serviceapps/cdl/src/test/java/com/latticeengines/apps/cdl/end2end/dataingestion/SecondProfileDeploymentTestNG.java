package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_2;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class SecondProfileDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint("consolidate2");
        verifySecondConsolidateCheckpoint();

        importData();
        profile();
        verifyProfile();

        verifySecondProfileCheckpoint();
        saveCheckpoint("profile2");
    }

    private void importData() throws Exception {
        mockVdbImport(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2, 100);
        mockVdbImport(BusinessEntity.Contact, CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2, 200);
        Thread.sleep(2000);
    }

    private void verifyProfile() {
        long numAccounts = ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2;
        long numContacts = CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2;
        long numProducts = PRODUCT_IMPORT_SIZE_1 + PRODUCT_IMPORT_SIZE_2;
        Map<TableRoleInCollection, Long> expectedCounts = ImmutableMap.of( //
                BusinessEntity.Account.getServingStore(), numAccounts,
                BusinessEntity.Contact.getServingStore(), numContacts,
                BusinessEntity.Product.getServingStore(), numProducts);
        verifyProfileReport(profileAppId, expectedCounts);
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(initialVersion.complement());
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);

        createTestSegments();
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

        RatingEngine ratingEngine = createRuleBasedRatingEngine();
        Map<RuleBucketName, Long> ratingCounts = ImmutableMap.of( //
                RuleBucketName.A, RATING_A_COUNT_2, //
                RuleBucketName.D, RATING_D_COUNT_2, //
                RuleBucketName.F, RATING_F_COUNT_2
        );
        verifyRatingEngineCount(ratingEngine.getId(), ratingCounts);
    }

}
