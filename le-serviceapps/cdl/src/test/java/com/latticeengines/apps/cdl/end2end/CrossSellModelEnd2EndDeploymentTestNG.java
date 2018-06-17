package com.latticeengines.apps.cdl.end2end;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.cdl.rating.CrossSellRatingConfig;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.testframework.exposed.proxy.pls.ModelSummaryProxy;

public class CrossSellModelEnd2EndDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CrossSellModelEnd2EndDeploymentTestNG.class);
    private static final boolean USE_EXISTING_TENANT = false;
    private static final String EXISTING_TENANT = "LETest1529193763286"; // LETest1528844192916-14

    private MetadataSegment targetSegment;
    private RatingEngine testRatingEngine;
    private AIModel testAIModel;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    private static final String targetProductId = "GMm4ZQnMOWpN8Gn7MhZLB7SrGmOss";
    private static final String trainingProductId = TRAINING_PRODUCT;
    private long targetCount;

    @BeforeClass(groups = { "end2end", "manual", "precheckin" })
    public void setup() {
    }

    /**
     * This test is part of CD pipeline
     */
    @Test(groups = "end2end")
    public void testFirstPurchase() throws Exception {
        log.info("Running testFirstPurchase");
        setupEnd2EndTestEnvironment();
        resumeCrossSellCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        attachProtectedProxy(modelSummaryProxy);
        setupTestSegment();
        setupAndRunModeling(ModelingStrategy.CROSS_SELL_FIRST_PURCHASE, PredictionType.EXPECTED_VALUE);
    }

    /**
     * This test is part of trunk health and CD pipeline
     */
    @Test(groups = { "precheckin" })
    public void testRepeatedPurchase() throws Exception {
        log.info("Running testRepeatedPurchase");
        setupEnd2EndTestEnvironment();
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        attachProtectedProxy(modelSummaryProxy);
        setupTestSegment();
        setupAndRunModeling(ModelingStrategy.CROSS_SELL_FIRST_PURCHASE, PredictionType.PROPENSITY);
    }

    /**
     * This test is for generating model artifacts for other tests
     */
    @Test(groups = "manual")
    public void manualTest() throws Exception {
        log.info("Running manualTest");
        if (USE_EXISTING_TENANT) {
            testBed.useExistingTenantAsMain(EXISTING_TENANT);
            testBed.switchToSuperAdmin();
            mainTestTenant = testBed.getMainTestTenant();
        } else {
            setupEnd2EndTestEnvironment();
            resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        }
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        attachProtectedProxy(modelSummaryProxy);
        setupTestSegment();
        setupAndRunModeling(ModelingStrategy.CROSS_SELL_FIRST_PURCHASE, PredictionType.EXPECTED_VALUE);
    }

    private void setupAndRunModeling(ModelingStrategy strategy, PredictionType predictionType) {
        setupTestRatingEngine(strategy, predictionType);
        verifyCounts(strategy);
        log.info("Start Cross Sell modeling ...");
        verifyBucketMetadataNotGenerated();
        String modelingWorkflowApplicationId = ratingEngineProxy.modelRatingEngine(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), "bnguyen@lattice-engines.com");
        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        testRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), testRatingEngine.getId());
        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        testAIModel = (AIModel) ratingEngineProxy.getRatingModel(mainTestTenant.getId(), testRatingEngine.getId(),
                testAIModel.getId());
        // Assert.assertEquals(testAIModel.getModelingJobStatus(),
        // completedStatus);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
        verifyBucketMetadataGenerated();
        Assert.assertEquals(
                ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), testRatingEngine.getId()).getStatus(),
                RatingEngineStatus.INACTIVE);
    }

    private void verifyBucketMetadataNotGenerated() {
        Map<Long, List<BucketMetadata>> bucketMetadataHistory = bucketedScoreProxy
                .getABCDBucketsByEngineId(mainTestTenant.getId(), testRatingEngine.getId());
        Assert.assertTrue(bucketMetadataHistory.isEmpty());
    }

    private void verifyBucketMetadataGenerated() {
        Map<Long, List<BucketMetadata>> bucketMetadataHistory = bucketedScoreProxy
                .getABCDBucketsByEngineId(mainTestTenant.getId(), testRatingEngine.getId());
        Assert.assertNotNull(bucketMetadataHistory);
        Assert.assertEquals(bucketMetadataHistory.size(), 1);
        log.info("time is " + bucketMetadataHistory.keySet().toString());
        List<BucketMetadata> latestBucketedMetadata = bucketedScoreProxy
                .getLatestABCDBucketsByEngineId(mainTestTenant.getId(), testRatingEngine.getId());
        Assert.assertEquals(targetCount, latestBucketedMetadata.stream().mapToLong(BucketMetadata::getNumLeads).sum(),
                "Sum of leads in BucketMetadata is not equal to the target count");
        log.info("bucket metadata is " + JsonUtils.serialize(latestBucketedMetadata));
    }

    private void setupTestSegment() {
        targetSegment = constructTargetSegment();
        targetSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), targetSegment);
    }

    private void setupTestRatingEngine(ModelingStrategy strategy, PredictionType predictionType) {
        log.info("Set up test artifacts for a " + strategy + " model in tenant " + mainTestTenant.getId());
        RatingEngine ratingEngine = constructRatingEngine(RatingEngineType.CROSS_SELL, targetSegment);
        CrossSellRatingConfig ratingConfig = new CrossSellRatingConfig(strategy);
        ratingEngine.setAdvancedRatingConfig(ratingConfig);
        testRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);
        log.info("Created rating engine " + testRatingEngine.getId());
        testAIModel = (AIModel) testRatingEngine.getActiveModel();
        configureCrossSellModel(testAIModel, predictionType, strategy, targetProductId, trainingProductId);

        testAIModel = (AIModel) ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), testRatingEngine.getId(),
                testAIModel.getId(), testAIModel);
        log.info("Updated rating model " + testAIModel.getId());
        log.info("/ratingengines/" + testRatingEngine.getId() + "/ratingmodels/" + testAIModel.getId());
    }

    private void verifyCounts(ModelingStrategy strategy) {
        targetCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), ModelingQueryType.TARGET);
        long trainingCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), ModelingQueryType.TRAINING);
        long eventCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), ModelingQueryType.EVENT);
        String errorMsg = "targetCount=" + targetCount //
                + " trainingCount=" + trainingCount //
                + " eventCount=" + eventCount;
        log.info(errorMsg);

        if (strategy == ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE) {
            Assert.assertEquals(targetCount, 141, errorMsg);
            Assert.assertEquals(trainingCount, 1039, errorMsg);
            Assert.assertEquals(eventCount, 113, errorMsg);
        } else {
            Assert.assertEquals(targetCount, 248, errorMsg);
            Assert.assertEquals(trainingCount, 1364, errorMsg);
            Assert.assertEquals(eventCount, 9, errorMsg);
        }
    }
}
