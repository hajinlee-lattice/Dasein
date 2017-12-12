package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_1;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import java.util.Map;

/**
 * Process Account, Contact and Product for a new tenant
 */
public class ProcessAccountDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ProcessAccountDeploymentTestNG.class);

    static final String CHECK_POINT = "process1";

    private static final int ACCOUNT_IMPORT_SIZE_1_1 = 350;
    private static final int ACCOUNT_IMPORT_SIZE_1_2 = 150;
    private static final int CONTACT_IMPORT_SIZE_1_1 = 600;
    private static final int CONTACT_IMPORT_SIZE_1_2 = 500;
    private static final int PRODUCT_IMPORT_SIZE_1_1 = 70;
    private static final int PRODUCT_IMPORT_SIZE_1_2 = 30;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        Assert.assertEquals(ACCOUNT_IMPORT_SIZE_1_1 + ACCOUNT_IMPORT_SIZE_1_2, ACCOUNT_IMPORT_SIZE_1);
        Assert.assertEquals(CONTACT_IMPORT_SIZE_1_1 + CONTACT_IMPORT_SIZE_1_2, CONTACT_IMPORT_SIZE_1);
        Assert.assertEquals(PRODUCT_IMPORT_SIZE_1_1 + PRODUCT_IMPORT_SIZE_1_2, PRODUCT_IMPORT_SIZE_1);

        importData();
        processAnalyze();
        verifyProcess();
        // saveCheckpoint(CHECK_POINT);
    }

    private void importData() throws Exception {
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Initialized.getName());
        mockVdbImport(BusinessEntity.Account, 0, ACCOUNT_IMPORT_SIZE_1_1);
        mockVdbImport(BusinessEntity.Contact, 0, CONTACT_IMPORT_SIZE_1_1);
        mockVdbImport(BusinessEntity.Product, 0, PRODUCT_IMPORT_SIZE_1_1);
        Thread.sleep(2000);
        mockVdbImport(BusinessEntity.Account, ACCOUNT_IMPORT_SIZE_1_1, ACCOUNT_IMPORT_SIZE_1_2);
        mockVdbImport(BusinessEntity.Contact, CONTACT_IMPORT_SIZE_1_1, CONTACT_IMPORT_SIZE_1_2);
        mockVdbImport(BusinessEntity.Product, PRODUCT_IMPORT_SIZE_1_1, PRODUCT_IMPORT_SIZE_1_2);
        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void verifyProcess() {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(DataCollection.Version.Green);

        long numAccounts = countTableRole(BusinessEntity.Account.getBatchStore());
        Assert.assertEquals(numAccounts, ACCOUNT_IMPORT_SIZE_1);
        long numContacts = countTableRole(BusinessEntity.Contact.getBatchStore());
        Assert.assertEquals(numContacts, CONTACT_IMPORT_SIZE_1);
        long numProducts = countTableRole(BusinessEntity.Product.getBatchStore());
        Assert.assertEquals(numProducts, PRODUCT_IMPORT_SIZE_1);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), ACCOUNT_IMPORT_SIZE_1);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), CONTACT_IMPORT_SIZE_1);

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
