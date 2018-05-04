package com.latticeengines.apps.cdl.end2end.dataingestion;

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

public class CrossSellModelEnd2EndDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CrossSellModelEnd2EndDeploymentTestNG.class);
    private static final boolean USE_EXISTING_TENANT = false;
    private static final String EXISTING_TENANT = "JLM1525220846205";

    private static final boolean MANUAL_TEST_USE_TRANSACTION_RESTRICTION = false;
    private static final boolean E2E_TEST_USE_TRANSACTION_RESTRICTION = false;

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

    private static final String targetProductId = TARGET_PRODUCT;
    private static final String trainingProductId = TRAINING_PRODUCT;
    private long targetCount;

    @BeforeClass(groups = { "end2end", "manual" })
    public void setup() {
    }

    /**
     * This test is part of CD pipeline
     */
    @Test(groups = "end2end")
    public void runTest() throws Exception {
        setupEnd2EndTestEnvironment();
        resumeVdbCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        attachProtectedProxy(modelSummaryProxy);
        setupBusinessCalendar();
        setupTestSegment(E2E_TEST_USE_TRANSACTION_RESTRICTION);
        setupAndRunModeling(E2E_TEST_USE_TRANSACTION_RESTRICTION);
    }

    /**
     * This test is for generating model artifacts for other tests
     */
    @Test(groups = "manual")
    public void manualTest() throws Exception {
        if (USE_EXISTING_TENANT) {
            testBed.useExistingTenantAsMain(EXISTING_TENANT);
            testBed.switchToSuperAdmin();
            mainTestTenant = testBed.getMainTestTenant();
        } else {
            setupEnd2EndTestEnvironment();
            resumeVdbCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        }
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        attachProtectedProxy(modelSummaryProxy);
        setupBusinessCalendar();

        setupTestSegment(MANUAL_TEST_USE_TRANSACTION_RESTRICTION);
        setupAndRunModeling(MANUAL_TEST_USE_TRANSACTION_RESTRICTION);
    }

    private void setupAndRunModeling(boolean txnRestrictionsUsed) {
        setupTestRatingEngine();
        verifyCounts(txnRestrictionsUsed);
        log.info("Start modeling ...");
        verifyBucketMetadataNotGenerated();
        String modelingWorkflowApplicationId = ratingEngineProxy.modelRatingEngine(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), "bnguyen@lattice-engines.com");
        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        testRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), testRatingEngine.getId());
        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
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

    private void setupTestSegment(boolean useTrxRestrictions) {
        targetSegment = useTrxRestrictions ? targetSegmentWithTrxRestrictions() : constructTargetSegment();

        targetSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), targetSegment);
    }

    private void setupTestRatingEngine() {
        log.info("Setting up test artifacts for tenant " + mainTestTenant.getId());
        RatingEngine ratingEngine = constructRatingEngine(RatingEngineType.CROSS_SELL, targetSegment);
        CrossSellRatingConfig ratingConfig = new CrossSellRatingConfig(ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE);
        ratingEngine.setAdvancedRatingConfig(ratingConfig);
        testRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);
        log.info("Created rating engine " + testRatingEngine.getId());

        testAIModel = (AIModel) testRatingEngine.getActiveModel();
        configureCrossSellModel(testAIModel, PredictionType.EXPECTED_VALUE, targetProductId, trainingProductId);

        testAIModel = (AIModel) ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), testRatingEngine.getId(),
                testAIModel.getId(), testAIModel);
        log.info("Updated rating model " + testAIModel.getId());
        log.info("/ratingengines/" + testRatingEngine.getId() + "/ratingmodels/" + testAIModel.getId());
    }

    private void verifyCounts(boolean txnRestrictionsUsed) {
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

        Assert.assertEquals(targetCount, txnRestrictionsUsed ? 55 : 81, errorMsg);
        Assert.assertEquals(trainingCount, txnRestrictionsUsed ? 298 : 557, errorMsg);
        Assert.assertEquals(eventCount, txnRestrictionsUsed ? 35 : 53, errorMsg);
    }
}
