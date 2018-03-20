package com.latticeengines.apps.cdl.end2end.dataingestion;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.CrossSellModelingConfigKeys;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.testframework.exposed.proxy.pls.ModelSummaryProxy;

/**
 * This test is for generating model artifacts for other tests
 */
public class CreateAIModelDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CreateAIModelDeploymentTestNG.class);
    private static final boolean USE_EXISTING_TENANT = false;
    private static final String EXISTING_TENANT = "JLM1521225340349";
    private static final boolean EV_MODEL = true;

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

    private final String targetProductId = "A74D1222394534E6B450CA006C20D48D";
    private final String trainingProductId = "A80D4770376C1226C47617C071324C0B";

    @BeforeClass(groups = { "end2end" })
    public void setup() throws Exception {
        if (USE_EXISTING_TENANT) {
            testBed.useExistingTenantAsMain(EXISTING_TENANT);
            testBed.switchToSuperAdmin();
            mainTestTenant = testBed.getMainTestTenant();
        } else {
            super.setup();
            resumeVdbCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        }
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        attachProtectedProxy(modelSummaryProxy);
        setupTestRatingEngine();
    }

    @AfterClass(groups = { "end2end" })
    public void cleanup() {
        ratingEngineProxy.deleteRatingEngine(mainTestTenant.getId(), testRatingEngine.getId());
        segmentProxy.deleteSegmentByName(mainTestTenant.getId(), testSegment.getName());
        if (trainSegment != null) {
            segmentProxy.deleteSegmentByName(mainTestTenant.getId(), trainSegment.getName());
        }
    }

    @Test(groups = "end2end")
    public void runTest() {
        log.info("Start modeling ...");
        String modelingWorkflowApplicationId = ratingEngineProxy.modelRatingEngine(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), "bnguyen@lattice-engines.com");
        log.info(String.format("Workflow application id is %s", modelingWorkflowApplicationId));
        testRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), testRatingEngine.getId());
        JobStatus completedStatus = waitForWorkflowStatus(modelingWorkflowApplicationId, false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    private void setupTestSegment() {
        testSegment = constructTargetSegment();
        testSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), testSegment);
    }

    private void setupTrainSegment() {
        trainSegment = constructTrainingSegment();
        trainSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(), trainSegment);
    }

    private void setupTestRatingEngine() {
        setupTestSegment();

        testRatingEngine = new RatingEngine();
        testRatingEngine.setDisplayName("CreateAIModelDeploymentTestRating");
        testRatingEngine.setTenant(mainTestTenant);
        testRatingEngine.setType(RatingEngineType.CROSS_SELL);
        testRatingEngine.setSegment(testSegment);
        testRatingEngine.setCreatedBy("bnguyen@lattice-engines.com");
        testRatingEngine.setCreated(new Date());
        testRatingEngine.setCreated(new Date());

        testRatingEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), testRatingEngine);
        testAIModel = (AIModel) testRatingEngine.getActiveModel();
        CrossSellModelingConfig advancedConf = CrossSellModelingConfig.getAdvancedModelingConfig(testAIModel);
        advancedConf.setModelingStrategy(ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE);
        Map<CrossSellModelingConfigKeys, ModelingConfigFilter> myMap = new HashMap<>();
        myMap.put(CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD, new ModelingConfigFilter(
                CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD, ComparisonType.PRIOR_ONLY, 6));
        CrossSellModelingConfig config = CrossSellModelingConfig.getAdvancedModelingConfig(testAIModel);
        config.setFilters(myMap);
        testAIModel.setPredictionType(PredictionType.EXPECTED_VALUE);
        config.setTargetProducts(Collections.singletonList(targetProductId));
        config.setTrainingProducts(Collections.singletonList(trainingProductId));
        testAIModel.setTrainingSegment(trainSegment);

        testAIModel = (AIModel) ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), testRatingEngine.getId(),
                testAIModel.getId(), testAIModel);

        long targetCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), ModelingQueryType.TARGET);
        Assert.assertEquals(targetCount, 19);

        long trainingCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), ModelingQueryType.TRAINING);
        Assert.assertEquals(trainingCount, 1219);

        long eventCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), ModelingQueryType.EVENT);
        Assert.assertEquals(eventCount, 135);
    }
}
