package com.latticeengines.apps.cdl.end2end;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.proxy.exposed.cdl.ActivityMetricsProxy;


public class UpdateTransactionDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(UpdateTransactionDeploymentTestNG.class);

    @Inject
    private ActivityMetricsProxy activityMetricsProxy;

    @Value("${common.test.pls.url}")
    private String deployedHostPort;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(UpdateContactDeploymentTestNG.CHECK_POINT);

        // To test deprecating curated metrics & action
        setupUpdatedPurchaseHistoryMetrics();

        Assert.assertEquals(countInRedshift(BusinessEntity.Account), 1000);
        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), 1000);

        new Thread(this::createTestSegments).start();

        importData();
        processAnalyze();
        verifyProcess();
    }

    private void importData() throws Exception {
        mockCSVImport(BusinessEntity.Transaction, 3, "Transaction");
        Thread.sleep(2000);
    }

    private void verifyProcess() {
        runCommonPAVerifications();
        verifyProcessAnalyzeReport(processAnalyzeAppId);

//        long numAccounts = ACCOUNT_IMPORT_SIZE_TOTAL;
//        long numContacts = CONTACT_IMPORT_SIZE_TOTAL;
//        long numProducts = PRODUCT_IMPORT_SIZE_1;
//        long numTransactions = TRANSACTION_IMPORT_SIZE_1 + TRANSACTION_IMPORT_SIZE_2;
//
//        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
//        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
//        Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()), numProducts);
//        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedRawTransaction), numTransactions);
//
//        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
//        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);

//        verityTestSegmentCountDiff(ImmutableList.of(BusinessEntity.Account, BusinessEntity.Contact));
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
//        Map<RatingBucketName, Long> ratingCounts = ImmutableMap.of( //
//                RatingBucketName.A, RATING_A_COUNT_2_REBUILD, //
//                RatingBucketName.D, RATING_D_COUNT_2_REBUILD, //
//                RatingBucketName.F, RATING_F_COUNT_2_REBUILD
//        );
        // TODO: use rating proxy
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
