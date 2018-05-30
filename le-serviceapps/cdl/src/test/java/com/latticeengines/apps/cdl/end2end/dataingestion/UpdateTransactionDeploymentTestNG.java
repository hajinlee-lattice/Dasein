package com.latticeengines.apps.cdl.end2end.dataingestion;

import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.ACCOUNT_IMPORT_SIZE_TOTAL;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.CONTACT_IMPORT_SIZE_TOTAL;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.PRODUCT_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.TRANSACTION_IMPORT_SIZE_1;
import static com.latticeengines.apps.cdl.end2end.dataingestion.CheckpointService.TRANSACTION_IMPORT_SIZE_2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.proxy.exposed.cdl.ActivityMetricsProxy;


public class UpdateTransactionDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(UpdateTransactionDeploymentTestNG.class);

    static final String CHECK_POINT = "update3";
    private RatingEngine ratingEngine;

    @Inject
    private ActivityMetricsProxy activityMetricsProxy;

    @Value("${common.test.pls.url}")
    private String deployedHostPort;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        
        resumeCheckpoint(UpdateContactDeploymentTestNG.CHECK_POINT);

        // To test deprecating curated metrics & action
        setupUpdatedPurchaseHistoryMetrics();

        long numAccounts = ACCOUNT_IMPORT_SIZE_TOTAL;
        long numContacts = CONTACT_IMPORT_SIZE_TOTAL;
        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);

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

    private void importData() throws Exception {
        mockVdbImport(BusinessEntity.Transaction, TRANSACTION_IMPORT_SIZE_1, TRANSACTION_IMPORT_SIZE_2);
        Thread.sleep(2000);
    }

    private void verifyProcess() {
        runCommonPAVerifications();

        verifyProcessAnalyzeReport(processAnalyzeAppId);

        long numAccounts = ACCOUNT_IMPORT_SIZE_TOTAL;
        long numContacts = CONTACT_IMPORT_SIZE_TOTAL;
        long numProducts = PRODUCT_IMPORT_SIZE_1;
        long numTransactions = TRANSACTION_IMPORT_SIZE_1 + TRANSACTION_IMPORT_SIZE_2;

        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
        Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()), numProducts);
        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedRawTransaction), numTransactions);

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);

        verityTestSegmentCountDiff(ImmutableList.of(BusinessEntity.Account, BusinessEntity.Contact));
        /*
        Map<BusinessEntity, Long> segment1Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_1_ACCOUNT_4,
                BusinessEntity.Contact, SEGMENT_1_CONTACT_4);
        verifyTestSegment1Counts(segment1Counts);
        Map<BusinessEntity, Long> segment2Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_2_ACCOUNT_2_REBUILD, BusinessEntity.Contact,
                SEGMENT_2_CONTACT_2_REBUILD);
        verifyTestSegment2Counts(segment2Counts);
        */
        Map<RatingBucketName, Long> ratingCounts = ImmutableMap.of( //
                RatingBucketName.A, RATING_A_COUNT_2_REBUILD, //
                RatingBucketName.D, RATING_D_COUNT_2_REBUILD, //
                RatingBucketName.F, RATING_F_COUNT_2_REBUILD
        );
        // TODO: Rating engine needs to be activated
        // verifyRatingEngineCount(ratingEngine.getId(), ratingCounts);
    }

    void setupUpdatedPurchaseHistoryMetrics() {
        // Deprecated all the selected metrics
        List<ActivityMetrics> metrics = new ArrayList<>();
        RestTemplate restTemplate = testBed.getRestTemplate();
        deployedHostPort = deployedHostPort.endsWith("/") ? deployedHostPort.substring(0, deployedHostPort.length() - 1)
                : deployedHostPort;
        restTemplate.postForObject(deployedHostPort + "/pls/datacollection/metrics/PurchaseHistory", metrics,
                List.class);
    }
}
