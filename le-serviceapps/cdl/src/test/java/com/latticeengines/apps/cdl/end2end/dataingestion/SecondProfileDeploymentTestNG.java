package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_2;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_2;

import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

public class SecondProfileDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private YarnConfiguration yarnConfiguration;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint("consolidate2");
        verifySecondConsolidateCheckpoint();

        dataFeedProxy.rebuildTransaction(mainTestTenant.getId(), Boolean.FALSE);
        dataFeedProxy.updateEarliestTransaction(mainTestTenant.getId(), 42967);
        Table rawTable = dataCollectionProxy.getTable(mainTestTenant.getId(), TableRoleInCollection.ConsolidatedRawTransaction);
        Integer earliestDayPeriod = TimeSeriesUtils.getEarliestPeriod(yarnConfiguration, rawTable);
        dataFeedProxy.updateEarliestTransaction(mainTestTenant.getId(), earliestDayPeriod);

        importData();
        profile();
        verifyProfile();

        verifySecondProfileCheckpoint();
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
                BusinessEntity.Product, numProducts);
        // verifyTestSegment1Counts(segment1Counts);
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
