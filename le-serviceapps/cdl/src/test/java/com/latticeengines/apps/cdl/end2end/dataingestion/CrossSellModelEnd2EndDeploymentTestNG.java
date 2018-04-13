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
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationSelector;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.testframework.exposed.proxy.pls.ModelSummaryProxy;


public class CrossSellModelEnd2EndDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CrossSellModelEnd2EndDeploymentTestNG.class);
    private static final boolean USE_EXISTING_TENANT = false;
    private static final String EXISTING_TENANT = "LETest1523523952103";

    private static final boolean MANUAL_TEST_USE_DATE_RESTRICTION = false;
    private static final PredictionType MANUAL_TEST_PREDICTION_TYPE = PredictionType.EXPECTED_VALUE;

    private MetadataSegment testSegment;
    private MetadataSegment trainSegment;
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
        setupTestSegment(false);
        testModelPredictionType(PredictionType.EXPECTED_VALUE, false);
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

        setupTestSegment(MANUAL_TEST_USE_DATE_RESTRICTION);
        testModelPredictionType(MANUAL_TEST_PREDICTION_TYPE, MANUAL_TEST_USE_DATE_RESTRICTION);
    }

    private void testModelPredictionType(PredictionType predictionType, boolean useDateRestriction) {
        setupTestRatingEngine(predictionType);
        verifyCounts(useDateRestriction);
        log.info("Start modeling ...");
        verifyBucketMetadataNotGenerated();
        String modelingWorkflowApplicationId = ratingEngineProxy.modelRatingEngine(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), "bnguyen@lattice-engines.com");
        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        testRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), testRatingEngine.getId());
        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
        verifyBucketMetadataGenerated();
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
        log.info("bucket metadata is " + JsonUtils.serialize(latestBucketedMetadata));
    }

    private void setupTestSegment(boolean useDateRestriction) {
        if (useDateRestriction) {
            testSegment = targetSegmentWithDataRestriction();
        } else {
            testSegment = constructTargetSegment();
        }
        testSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), testSegment);
    }

    private void setupTrainSegment() {
        trainSegment = constructTrainingSegment();
        trainSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), trainSegment);
    }

    private void setupTestRatingEngine(PredictionType predictionType) {
        RatingEngine ratingEngine = constructRatingEngine(RatingEngineType.CROSS_SELL, testSegment);
        testRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);
        log.info("Created rating engine " + testRatingEngine.getId());

        testAIModel = (AIModel) testRatingEngine.getActiveModel();
        configureCrossSellModel(testAIModel, predictionType, targetProductId, trainingProductId);
        testAIModel.setTrainingSegment(trainSegment);

        testAIModel = (AIModel) ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), testRatingEngine.getId(),
                testAIModel.getId(), testAIModel);
        log.info("Updated rating model " + testAIModel.getId());
    }

    // TODO: will enable this kind of target segment after DP-5868 is fixed
    private MetadataSegment targetSegmentWithDataRestriction() {
        Bucket stateBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList("No"));
        BucketRestriction accountRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "OUT_OF_BUSINESS_INDICATOR"), stateBkt);

        TransactionRestriction trxRes = new TransactionRestriction("1E26CD1E01559048FF7B51ADA27EA7AB",
                new TimeFilter(ComparisonType.BEFORE, PeriodStrategy.Template.Date.name(),
                        Collections.singletonList("2018-04-09")),
                false,
                new AggregationFilter(AggregationSelector.SPENT, AggregationType.SUM, ComparisonType.GREATER_THAN,
                        Collections.singletonList(1)),
                new AggregationFilter(AggregationSelector.UNIT, AggregationType.SUM, ComparisonType.GREATER_THAN,
                        Collections.singletonList(1)));

        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_MODELING);
        segment.setDisplayName("End2End Segment Modeling");
        segment.setDescription("A test segment for CDL end2end modeling test.");
        segment.setAccountFrontEndRestriction(
                new FrontEndRestriction(Restriction.builder().and(accountRestriction, trxRes).build()));
        segment.setAccountRestriction(Restriction.builder().and(accountRestriction, trxRes).build());
        return segment;
    }

    private void setupBusinessCalendar() {
        periodProxy.saveBusinessCalendar(mainTestTenant.getId(), getStartingDateBusinessCalendderForTest());
    }

    private void verifyCounts(boolean useDateRestriction) {
        long targetCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), ModelingQueryType.TARGET);
        long trainingCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), ModelingQueryType.TRAINING);
        long eventCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), ModelingQueryType.EVENT);
        String errorMsg = "targetCount=" + targetCount //
                + " trainingCount=" + trainingCount //
                + " eventCount=" + eventCount;

        if (useDateRestriction) {
            Assert.assertEquals(targetCount, 22, errorMsg);
        } else {
            Assert.assertEquals(targetCount, 87, errorMsg);
        }
        Assert.assertEquals(trainingCount, 581, errorMsg);
        Assert.assertEquals(eventCount, 56, errorMsg);
    }
}
