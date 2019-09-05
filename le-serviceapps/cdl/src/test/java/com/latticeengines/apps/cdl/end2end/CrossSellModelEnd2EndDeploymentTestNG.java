package com.latticeengines.apps.cdl.end2end;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ApprovedUsage;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
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
    private static final String EXISTING_TENANT = "JLMTest1548550277029"; // LETest1528844192916-14

    private static final String LOADING_CHECKPOINT = UpdateTransactionDeploymentTestNG.CHECK_POINT;

    private MetadataSegment targetSegment;
    private RatingEngine testModel;
    private AIModel testIteration1;
    private AIModel testIteration2;
    private final Map<String, Category> refinedAttributes = new HashMap<>();

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    // Target Products are shared with RefreshRatingDeploymentTestNG
    private static final ImmutableList<String> repeatTargetProducts = ImmutableList.of(
            "1iHa3C9UQFBPknqKCNW3L6WgUAARc4o", "wvgdBuktiOzn3Vb5NMjEV9PnM1CZwluO", "f5v635l5ry87S2sJlinOMBey6YwjKzb",
            "AdbFXykhUy2A4FPiqP2lEXGbBhd8xuTf");
    private static final ImmutableList<String> firstTargetProducts = ImmutableList
            .of("1iHa3C9UQFBPknqKCNW3L6WgUAARc4o");

    // Training Products are only used by this test
    private static final ImmutableList<String> repeatTrainingProducts = ImmutableList.of(
            "PQ8ho4EWIq2BpgpMul928bTgYiVoISna", "Og8oP4j5zJ1Lieh3G38qTINC6m2Jor", "NlRV90sf5DW24uZJN7rb1jHOCYxbgtK5", //
            "NlRV90sf5DW24uZJN7rb1jHOCYxbgtK5", "AA2AE7655F28BFE08EC4D4CB387DB488", //
            "CrqP4hEDn3tMF3ZConN0j6CVchfOWRAq", "g8cH04Lzvb0Mhou2lvuuSJjjvQm1KQ3J", //
            "650050C066EF46905EC469E9CC2921E0", "26OcFFnMiag2jvSb3iucbyiwJuxPVY1P");
    private static final ImmutableList<String> firstTrainingProducts = ImmutableList
            .of("PQ8ho4EWIq2BpgpMul928bTgYiVoISna");

    private long targetCount;

    @BeforeClass(groups = { "end2end", "manual", "precheckin" })
    public void setup() {
    }

    /**
     * This test is part of trunk health and CD pipeline
     */
    @Test(groups = { "precheckin" })
    public void testFirstPurchase() throws Exception {
        log.info("Running testFirstPurchase");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), false);
        setupEnd2EndTestEnvironment(featureFlagMap);
        resumeCrossSellCheckpoint(LOADING_CHECKPOINT);
        attachProtectedProxy(modelSummaryProxy);
        setupTestSegment();
        setupAndRunModel(ModelingStrategy.CROSS_SELL_FIRST_PURCHASE, PredictionType.EXPECTED_VALUE, true);
    }

    /**
     * This test is part of CD pipeline
     */
    @Test(groups = "end2end")
    public void testRepeatedPurchase() throws Exception {
        log.info("Running testRepeatedPurchase");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), false);
        setupEnd2EndTestEnvironment(featureFlagMap);
        resumeCrossSellCheckpoint(LOADING_CHECKPOINT);
        attachProtectedProxy(modelSummaryProxy);
        setupTestSegment();
        setupAndRunModel(ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE, PredictionType.PROPENSITY, true);
        setupAndRunRemodel(PredictionType.PROPENSITY);
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
            Map<String, Boolean> featureFlagMap = new HashMap<>();
            featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), false);
            setupEnd2EndTestEnvironment(featureFlagMap);
            resumeCrossSellCheckpoint(LOADING_CHECKPOINT);
        }
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        attachProtectedProxy(modelSummaryProxy);
        setupTestSegment();
        setupAndRunModel(ModelingStrategy.CROSS_SELL_FIRST_PURCHASE, PredictionType.PROPENSITY, false);
        setupAndRunModel(ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE, PredictionType.EXPECTED_VALUE, false);
    }

    private void setupAndRunModel(ModelingStrategy strategy, PredictionType predictionType, boolean testInvalidEngine) {
        setupTestRatingEngine(strategy, predictionType, testInvalidEngine);
        verifyCounts(strategy);
        log.info("Start Cross Sell modeling ...");
        verifyBucketMetadataNotGenerated();
        String modelingWorkflowApplicationId = ratingEngineProxy.modelRatingEngine(mainTestTenant.getId(),
                testModel.getId(), testIteration1.getId(), null, "ga_dev@lattice-engines.com");
        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        testModel = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), testModel.getId());
        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        testIteration1 = (AIModel) ratingEngineProxy.getRatingModel(mainTestTenant.getId(), testModel.getId(),
                testIteration1.getId());
        Assert.assertEquals(testIteration1.getModelingJobStatus(), completedStatus);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
        verifyBucketMetadataGenerated(predictionType);
        Assert.assertEquals(ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), testModel.getId()).getStatus(),
                RatingEngineStatus.INACTIVE);
        verifyModelSummary(testIteration1.getModelSummaryId(), predictionType);
    }

    private void verifyModelSummary(String modelSummaryId, PredictionType predictionType) {
        ModelSummary modelSummary = modelSummaryProxy.getModelSummary(modelSummaryId);
        Assert.assertNotNull(modelSummary);
        Assert.assertNotNull(modelSummary.getId());
        if (predictionType == PredictionType.EXPECTED_VALUE) {
            Assert.assertNotNull(modelSummary.getAverageRevenue());
            Assert.assertNotNull(modelSummary.getAverageRevenueTestDataset());
            Assert.assertNotNull(modelSummary.getNormalizationRatio());
        } else {
            Assert.assertNull(modelSummary.getAverageRevenue());
            Assert.assertNull(modelSummary.getAverageRevenueTestDataset());
            Assert.assertNull(modelSummary.getNormalizationRatio());
        }
    }

    private void setupAndRunRemodel(PredictionType predictionType) {
        log.info("Starting Cross sell remodeling ...");
        testIteration2 = new AIModel();
        testIteration2.setRatingEngine(testModel);
        testIteration2.setAdvancedModelingConfig(testIteration1.getAdvancedModelingConfig());
        testIteration2.setDerivedFromRatingModel(testIteration1.getId());
        testIteration2.setPredictionType(predictionType);
        testIteration2 = (AIModel) ratingEngineProxy.createModelIteration(mainTestTenant.getId(), testModel.getId(),
                testIteration2);

        testModel = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), testModel.getId());
        Assert.assertEquals(testModel.getLatestIteration().getId(), testIteration2.getId());

        List<ColumnMetadata> attrs = ratingEngineProxy.getIterationMetadata(mainTestTenant.getId(), testModel.getId(),
                testIteration1.getId(), null);
        Assert.assertNotNull(attrs);

        verifyBucketMetadataGenerated(predictionType);

        String modelingWorkflowApplicationId = ratingEngineProxy.modelRatingEngine(mainTestTenant.getId(),
                testModel.getId(), testIteration2.getId(), refineAttributes(attrs), "some@email.com");
        log.info(String.format("Remodel workflow application id is %s", modelingWorkflowApplicationId));
        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
        testIteration2 = (AIModel) ratingEngineProxy.getRatingModel(mainTestTenant.getId(), testModel.getId(),
                testIteration2.getId());
        Assert.assertEquals(testIteration2.getModelingJobStatus(), completedStatus);
        Assert.assertEquals(ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), testModel.getId()).getStatus(),
                RatingEngineStatus.INACTIVE);

        attrs = ratingEngineProxy.getIterationMetadata(mainTestTenant.getId(), testModel.getId(),
                testIteration2.getId(), null);
        Assert.assertNotNull(attrs);

        verifyRefinedAttributes(attrs);
    }

    private void verifyBucketMetadataNotGenerated() {
        Map<Long, List<BucketMetadata>> bucketMetadataHistory = bucketedScoreProxy
                .getABCDBucketsByEngineId(mainTestTenant.getId(), testModel.getId());
        Assert.assertTrue(bucketMetadataHistory.isEmpty());
    }

    private void verifyBucketMetadataGenerated(PredictionType predictionType) {
        Map<Long, List<BucketMetadata>> bucketMetadataHistory = bucketedScoreProxy
                .getABCDBucketsByEngineId(mainTestTenant.getId(), testModel.getId());
        Assert.assertNotNull(bucketMetadataHistory);
        Assert.assertEquals(bucketMetadataHistory.size(), 1);
        log.info("time is " + bucketMetadataHistory.keySet().toString());
        List<BucketMetadata> latestBucketedMetadata = bucketMetadataHistory.values().iterator().next();
        Assert.assertEquals(targetCount, latestBucketedMetadata.stream().mapToLong(BucketMetadata::getNumLeads).sum(),
                "Sum of leads in BucketMetadata is not equal to the target count");
        log.info("bucket metadata is " + JsonUtils.serialize(latestBucketedMetadata));
        latestBucketedMetadata.stream().forEach(bucket -> {
            if (predictionType == PredictionType.EXPECTED_VALUE) {
                Assert.assertNotNull(bucket.getAverageExpectedRevenue());
                Assert.assertNotNull(bucket.getTotalExpectedRevenue());
            } else {
                Assert.assertNull(bucket.getAverageExpectedRevenue());
                Assert.assertNull(bucket.getTotalExpectedRevenue());
            }
        });
    }

    private void setupTestSegment() {
        targetSegment = constructTargetSegment();
        targetSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), targetSegment);
    }

    private void setupTestRatingEngine(ModelingStrategy strategy, PredictionType predictionType, //
            boolean testInvalidEngine) {
        log.info("Set up test artifacts for a " + strategy + " model in tenant " + mainTestTenant.getId());
        RatingEngine ratingEngine = constructRatingEngine(RatingEngineType.CROSS_SELL, targetSegment);
        CrossSellRatingConfig ratingConfig = new CrossSellRatingConfig(strategy);
        ratingEngine.setAdvancedRatingConfig(ratingConfig);
        testModel = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);
        log.info("Created rating engine " + testModel.getId());
        testIteration1 = (AIModel) testModel.getLatestIteration();

        if (testInvalidEngine) {
            Assert.assertThrows(LedpException.class,
                    () -> ratingEngineProxy.validateForModelingByRatingEngineId(mainTestTenant.getId(),
                            testModel.getId(), testIteration1.getId()));
        }

        List<String> targetProducts = ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE.equals(strategy)
                ? repeatTargetProducts
                : firstTargetProducts;
        List<String> trainingProducts = ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE.equals(strategy)
                ? repeatTrainingProducts
                : firstTrainingProducts;
        configureCrossSellModel(testIteration1, predictionType, strategy, targetProducts, trainingProducts);

        testIteration1 = (AIModel) ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), testModel.getId(),
                testIteration1.getId(), testIteration1);

        Assert.assertTrue(ratingEngineProxy.validateForModelingByRatingEngineId(mainTestTenant.getId(),
                testModel.getId(), testIteration1.getId()));

        log.info("Updated rating model " + testIteration1.getId());
        log.info("/ratingengines/" + testModel.getId() + "/ratingmodels/" + testIteration1.getId());
    }

    private void verifyCounts(ModelingStrategy strategy) {
        log.info("Verifying counts ...");
        targetCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(), testModel.getId(),
                testIteration1.getId(), ModelingQueryType.TARGET);
        long trainingCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                testModel.getId(), testIteration1.getId(), ModelingQueryType.TRAINING);
        long eventCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(), testModel.getId(),
                testIteration1.getId(), ModelingQueryType.EVENT);
        String errorMsg = "targetCount=" + targetCount //
                + " trainingCount=" + trainingCount //
                + " eventCount=" + eventCount;
        if (strategy == ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE) {
            Assert.assertEquals(targetCount, 22, errorMsg);
            Assert.assertEquals(trainingCount, 232, errorMsg);
            Assert.assertEquals(eventCount, 193, errorMsg);
        } else {
            Assert.assertEquals(targetCount, 554, errorMsg);
            Assert.assertEquals(trainingCount, 2618, errorMsg);
            Assert.assertEquals(eventCount, 68, errorMsg);
        }
    }

    private void verifyRefinedAttributes(List<ColumnMetadata> attrs) {
        for (String refinedAttribute : refinedAttributes.keySet()) {
            ColumnMetadata cm = attrs.stream().filter(attr -> attr.getAttrName().equals(refinedAttribute)).findFirst()
                    .get();
            Assert.assertEquals(cm.getApprovedUsageList().size(), 1);
            Assert.assertEquals(cm.getApprovedUsageList().get(0), ApprovedUsage.NONE);
        }
    }

    private List<ColumnMetadata> refineAttributes(List<ColumnMetadata> attrs) {
        int noOfAttributesToRefine = 3;
        for (ColumnMetadata attr : attrs) {
            if (attr.getImportanceOrdering() != null) {
                refinedAttributes.put(attr.getAttrName(), attr.getCategory());
                attr.setApprovedUsageList(Arrays.asList(ApprovedUsage.NONE));
                noOfAttributesToRefine--;
            }
            if (noOfAttributesToRefine == 0) {
                return attrs;
            }
        }

        return attrs;
    }
}
