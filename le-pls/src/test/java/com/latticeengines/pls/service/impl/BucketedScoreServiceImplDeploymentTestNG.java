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
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.pls.controller.RatingEngineResourceDeploymentTestNG;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
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

        segment = new MetadataSegment();
        Restriction accountRestriction = RatingEngineResourceDeploymentTestNG.getTestRestriction();
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        segment.setContactFrontEndRestriction(new FrontEndRestriction());
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
        aiModel.setTargetProducts(Collections.singletonList(TARGET_PRODUCT));
        aiModel.setPredictionType(predictionType);
        aiModel.setModelingStrategy(ModelingStrategy.CROSS_SELL_FIRST_PURCHASE);
        return aiModel;
    }

    @Test(groups = { "deployment" })
    public void testCreationFirstSetOfBuckets() {
        ModelSummary modelSummary = modelSummaryService.getModelSummary(modelId);
        long oldLastUpdateTime = modelSummary.getLastUpdateTime();
        System.out.println("oldLastUpdateTime is " + oldLastUpdateTime);
        System.out.println("current time  is " + System.currentTimeMillis());
        bucketedScoreService.createBucketMetadatas(ratingEngine.getId(), aiModel.getId(),
                Arrays.asList(BucketedScoreServiceTestUtils.bucketMetadata1), CREATED_BY);

        Map<Long, List<BucketMetadata>> creationTimeToBucketMetadatas = bucketedScoreService
                .getModelBucketMetadataGroupedByCreationTimesBasedOnRatingEngineId(ratingEngine.getId());
        Long timestamp = (Long) creationTimeToBucketMetadatas.keySet().toArray()[0];
        modelSummary = modelSummaryService.getModelSummary(modelId);
        long newLastUpdateTime = modelSummary.getLastUpdateTime();
        System.out.println("newLastUpdateTime is " + newLastUpdateTime);
        assertTrue(newLastUpdateTime > oldLastUpdateTime);
        BucketedScoreServiceTestUtils.testFirstGroupBucketMetadata(creationTimeToBucketMetadatas.get(timestamp));
    }

    @Test(groups = { "deployment" }, dependsOnMethods = "testCreationFirstSetOfBuckets")
    public void testCreationSecondSetOfBuckets() throws Exception {
        ModelSummary modelSummary = modelSummaryService.getModelSummary(modelId);
        long oldLastUpdateTime = modelSummary.getLastUpdateTime();
        bucketedScoreService.createBucketMetadatas(ratingEngine.getId(), aiModel.getId(),
                Arrays.asList(BucketedScoreServiceTestUtils.bucketMetadata2), CREATED_BY);
        modelSummary = modelSummaryService.getModelSummary(modelId);
        long newLastUpdateTime = modelSummary.getLastUpdateTime();
        assertTrue(newLastUpdateTime > oldLastUpdateTime);

        Map<Long, List<BucketMetadata>> creationTimeToBucketMetadatas = bucketedScoreService
                .getModelBucketMetadataGroupedByCreationTimesBasedOnRatingEngineId(ratingEngine.getId());
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
    }

    @Test(groups = { "deployment" }, dependsOnMethods = "testCreationSecondSetOfBuckets")
    public void testGetUpToDateModelBucketMetadata() throws Exception {
        List<BucketMetadata> bucketMetadatas = bucketedScoreService
                .getUpToDateABCDBucketsBasedOnRatingEngineId(ratingEngine.getId());
        BucketedScoreServiceTestUtils.testSecondGroupBucketMetadata(bucketMetadatas);
    }

}
