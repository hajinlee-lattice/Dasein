package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.UuidUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.pls.controller.RatingEngineResourceDeploymentTestNG;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.pls.service.ActionService;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.MetadataSegmentService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;

public class BucketedScoreServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(BucketedScoreServiceImplDeploymentTestNG.class);

    @Inject
    private MetadataSegmentService metadataSegmentService;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private BucketedScoreService bucketedScoreService;

    @Inject
    private ActionService actionService;

    private static final String SEGMENT_NAME = "segment";

    static final String TARGET_PRODUCT = "B91D4770376C1226C47617C071324C0B";

    private static final String CREATED_BY = "lattice@lattice-engines.com";

    private MetadataSegment segment;

    private AIModel aiModel;

    private String modelId;

    private RatingEngine ratingEngine;

    private ModelSummary modelSummary;

    @Inject
    private ModelSummaryService modelSummaryService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
        MultiTenantContext.setTenant(mainTestTenant);

        modelId = UuidUtils.shortenUuid(UUID.randomUUID());
        modelSummary = BucketedScoreServiceTestUtils.createModelSummary(modelId);
        modelSummaryService.createModelSummary(modelSummary, mainTestTenant.getId());

        segment = RatingEngineResourceDeploymentTestNG.constructSegment(SEGMENT_NAME);
        segment.setDisplayName(SEGMENT_NAME);
        MetadataSegment createdSegment = metadataSegmentService.createOrUpdateSegment(segment);
        Assert.assertNotNull(createdSegment);
        MetadataSegment retrievedSegment = metadataSegmentService.getSegmentByName(createdSegment.getName(), false);
        log.info(String.format("Created metadata segment with name %s", retrievedSegment.getName()));

        RatingEngine re = new RatingEngine();
        re.setSegment(retrievedSegment);
        re.setCreatedBy(CREATED_BY);
        re.setType(RatingEngineType.CROSS_SELL);
        ratingEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), re);
        Assert.assertNotNull(ratingEngine);
        aiModel = createAIModel((AIModel) ratingEngine.getActiveModel(), modelSummary, PredictionType.EXPECTED_VALUE);
        ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), ratingEngine.getId(), aiModel.getId(), aiModel);
    }

    private AIModel createAIModel(AIModel aiModel, ModelSummary modelSummary, PredictionType predictionType) {
        aiModel.setModelSummary(modelSummary);
        CrossSellModelingConfig advancedModelingConf = CrossSellModelingConfig.getAdvancedModelingConfig(aiModel);
        advancedModelingConf.setTargetProducts(Collections.singletonList(TARGET_PRODUCT));
        aiModel.setPredictionType(predictionType);
        advancedModelingConf.setModelingStrategy(ModelingStrategy.CROSS_SELL_FIRST_PURCHASE);
        return aiModel;
    }

    @Test(groups = { "deployment" })
    public void testCreationFirstSetOfBuckets() {
        ModelSummary modelSummary = modelSummaryService.getModelSummary(modelId);
        long oldLastUpdateTime = modelSummary.getLastUpdateTime();
        System.out.println("oldLastUpdateTime is " + oldLastUpdateTime);
        System.out.println("current time  is " + System.currentTimeMillis());
        createBucketMetadatas(ratingEngine.getId(), aiModel.getId(),
                Arrays.asList(BucketedScoreServiceTestUtils.bucketMetadata1));

        Map<Long, List<BucketMetadata>> creationTimeToBucketMetadatas = getModelBucketMetadataGroupedByCreationTimesBasedOnRatingEngineId(
                ratingEngine.getId());
        Long timestamp = (Long) creationTimeToBucketMetadatas.keySet().toArray()[0];
        modelSummary = modelSummaryService.getModelSummary(modelId);
        long newLastUpdateTime = modelSummary.getLastUpdateTime();
        System.out.println("newLastUpdateTime is " + newLastUpdateTime);
        assertTrue(newLastUpdateTime > oldLastUpdateTime);
        BucketedScoreServiceTestUtils.testFirstGroupBucketMetadata(creationTimeToBucketMetadatas.get(timestamp));
        testCreationOfFirstAction();
    }

    @Test(groups = { "deployment" }, dependsOnMethods = "testCreationFirstSetOfBuckets")
    public void testCreationSecondSetOfBuckets() throws Exception {
        ModelSummary modelSummary = modelSummaryService.getModelSummary(modelId);
        long oldLastUpdateTime = modelSummary.getLastUpdateTime();
        createBucketMetadatas(ratingEngine.getId(), aiModel.getId(),
                Arrays.asList(BucketedScoreServiceTestUtils.bucketMetadata2));
        modelSummary = modelSummaryService.getModelSummary(modelId);
        long newLastUpdateTime = modelSummary.getLastUpdateTime();
        assertTrue(newLastUpdateTime > oldLastUpdateTime);

        Map<Long, List<BucketMetadata>> creationTimeToBucketMetadatas = getModelBucketMetadataGroupedByCreationTimesBasedOnRatingEngineId(
                ratingEngine.getId());
        assertEquals(creationTimeToBucketMetadatas.keySet().size(), 2);
        Long earlierTimestamp = (Long) creationTimeToBucketMetadatas.keySet().toArray()[0],
                laterTimestamp = (Long) creationTimeToBucketMetadatas.keySet().toArray()[1];
        Long placeHolderTimestamp;
        if (earlierTimestamp > laterTimestamp) {
            placeHolderTimestamp = earlierTimestamp;
            earlierTimestamp = laterTimestamp;
            laterTimestamp = placeHolderTimestamp;
        }

        BucketedScoreServiceTestUtils.testFirstGroupBucketMetadata(creationTimeToBucketMetadatas.get(earlierTimestamp));
        BucketedScoreServiceTestUtils.testSecondGroupBucketMetadata(creationTimeToBucketMetadatas.get(laterTimestamp));
        testCreationOfSecondAction();
    }

    @Test(groups = { "deployment" }, dependsOnMethods = "testCreationSecondSetOfBuckets")
    public void testGetUpToDateModelBucketMetadata() throws Exception {
        List<BucketMetadata> bucketMetadatas = getUpToDateABCDBucketsBasedOnRatingEngineId(ratingEngine.getId());
        BucketedScoreServiceTestUtils.testSecondGroupBucketMetadata(bucketMetadatas);
    }

    // AI_MODEL_BUCKET_CHANGE
    private void testCreationOfFirstAction() {
        List<Action> actions = actionService.findAll();
        Assert.assertEquals(actions.size(), 1);
        Action action = actions.get(0);
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.RATING_ENGINE_CHANGE);
        Assert.assertNotNull(action.getDescription());
        log.info("AI_MODEL_BUCKET_CHANGE description is " + action.getDescription());

        RatingEngine retrievedRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(),
                ratingEngine.getId());
        Assert.assertEquals(retrievedRatingEngine.getStatus(), RatingEngineStatus.ACTIVE);
    }

    private void testCreationOfSecondAction() {
        List<Action> actions = actionService.findAll();
        Assert.assertEquals(actions.size(), 2);
        Action action = actions.get(1);
        Assert.assertNotNull(action);
        Assert.assertEquals(action.getType(), ActionType.RATING_ENGINE_CHANGE);
        Assert.assertNotNull(action.getDescription());
        log.info("AI_MODEL_BUCKET_CHANGE description is " + action.getDescription());

        RatingEngine retrievedRatingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(),
                ratingEngine.getId());
        Assert.assertEquals(retrievedRatingEngine.getStatus(), RatingEngineStatus.ACTIVE);
    }

    protected void createBucketMetadatas(String ratingEngineId, String ratingModelId,
            List<BucketMetadata> bucketMetadata) {
        bucketedScoreService.createBucketMetadatas(ratingEngineId, ratingModelId, bucketMetadata, CREATED_BY);
    }

    protected Map<Long, List<BucketMetadata>> getModelBucketMetadataGroupedByCreationTimesBasedOnRatingEngineId(
            String ratingEnigneId) {
        return bucketedScoreService.getModelBucketMetadataGroupedByCreationTimesBasedOnRatingEngineId(ratingEnigneId);
    }

    protected List<BucketMetadata> getUpToDateABCDBucketsBasedOnRatingEngineId(String ratingEngineId) {
        return bucketedScoreService.getUpToDateABCDBucketsBasedOnRatingEngineId(ratingEngineId);
    }

}
