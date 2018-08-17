package com.latticeengines.apps.cdl.end2end;

import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.lp.CreateBucketMetadataRequest;
import com.latticeengines.domain.exposed.util.BucketMetadataUtils;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.lp.BucketedScoreProxy;
import com.latticeengines.testframework.exposed.proxy.pls.ModelSummaryProxy;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class RefreshRatingDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RefreshRatingDeploymentTestNG.class);

    private static final boolean USE_EXISTING_TENANT = false;
    private static final String EXISTING_TENANT = "JLM1533618545277";

    private static final String LOADING_CHECKPOINT = UpdateTransactionDeploymentTestNG.CHECK_POINT;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

    @Value("${camille.zk.pod.id}")
    private String podId;

    private static final String MODELS_RESOURCE_ROOT = "end2end/models";
    private static final boolean ENABLE_AI_RATINGS = true;

    // Target Products are shared with Refresh Rating test
    private static final ImmutableList<String> targetProducts = ImmutableList.of("6aWAxPIdKjD9bDVN90kMphZgevl8jua");

    private RatingEngine rule1;
    private RatingEngine rule2;
    private RatingEngine rule3;
    private RatingEngine ai1;
    private RatingEngine ai2;
    private RatingEngine ai3;

    private String uuid1;
    private String uuid2;
    private String uuid3;

    @BeforeClass(groups = "end2end")
    public void setup() throws Exception {
        setup(USE_EXISTING_TENANT, ENABLE_AI_RATINGS);
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    @Test(groups = "end2end")
    public void runTest() {
        processAnalyze(constructRequest());
        verifyProcess();
    }

    private void setup(boolean useExistingTenant, boolean enableAIRatings) throws Exception {
        if (useExistingTenant) {
            testBed.useExistingTenantAsMain(EXISTING_TENANT);
            testBed.switchToSuperAdmin();
            mainTestTenant = testBed.getMainTestTenant();
            initialVersion = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        } else {
            setupEnd2EndTestEnvironment();
            setupBusinessCalendar();
            if (enableAIRatings) {
                new Thread(this::setupAIModels).start();
            }
            resumeCrossSellCheckpoint(LOADING_CHECKPOINT);
            verifyStats(false, BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.PurchaseHistory);

            new Thread(() -> {
                createTestSegment2();
                rule1 = createRuleBasedRatingEngine();
                rule2 = createRuleBasedRatingEngine();
                rule3 = createRuleBasedRatingEngine();
                activateRatingEngine(rule1.getId());
                activateRatingEngine(rule2.getId());
            }).start();

            if (enableAIRatings) {
                createModelingSegment();
                MetadataSegment segment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(),
                        SEGMENT_NAME_MODELING);
                Assert.assertNotNull(segment);

                ModelSummary modelSummary = waitToDownloadModelSummaryWithUuid(modelSummaryProxy, uuid1);
                ai1 = createCrossSellEngine(segment, modelSummary, PredictionType.EXPECTED_VALUE);
                long targetCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                        ai1.getId(), ai1.getActiveModel().getId(), ModelingQueryType.TARGET);
                Assert.assertTrue(targetCount > 100);
                activateRatingEngine(ai1.getId());

                modelSummary = waitToDownloadModelSummaryWithUuid(modelSummaryProxy, uuid2);
                ai2 = createCrossSellEngine(segment, modelSummary, PredictionType.PROPENSITY);
                targetCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(), ai2.getId(),
                        ai2.getActiveModel().getId(), ModelingQueryType.TARGET);
                Assert.assertTrue(targetCount > 100);
                activateRatingEngine(ai2.getId());

                modelSummary = waitToDownloadModelSummaryWithUuid(modelSummaryProxy, uuid3);
                ai3 = createCustomEventEngine(segment, modelSummary);
                activateRatingEngine(ai3.getId());
            }
        }
    }

    private void setupAIModels() {
        testBed.attachProtectedProxy(modelSummaryProxy);
        testBed.switchToSuperAdmin();
        uuid1 = uploadModel(MODELS_RESOURCE_ROOT + "/ev_model.tar.gz");
        uuid2 = uploadModel(MODELS_RESOURCE_ROOT + "/propensity_model.tar.gz");
        uuid3 = uploadModel(MODELS_RESOURCE_ROOT + "/ce_model.tar.gz");
    }

    private RatingEngine createCrossSellEngine(MetadataSegment segment, ModelSummary modelSummary,
            PredictionType predictionType) throws InterruptedException {
        RatingEngine ratingEngine = constructRatingEngine(RatingEngineType.CROSS_SELL, segment);

        RatingEngine newEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);
        newEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
        assertNotNull(newEngine);
        Assert.assertNotNull(newEngine.getActiveModel(), JsonUtils.pprint(newEngine));
        log.info("Created rating engine " + newEngine.getId());

        AIModel model = (AIModel) newEngine.getLatestIteration();
        configureCrossSellModel(model, predictionType, ModelingStrategy.CROSS_SELL_FIRST_PURCHASE, targetProducts,
                targetProducts);
        model.setModelSummaryId(modelSummary.getId());

        ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), newEngine.getId(), model.getId(), model);
        log.info("Updated rating model " + model.getId());

        final String modelGuid = modelSummary.getId();
        final String engineId = newEngine.getId();
        new Thread(() -> insertBucketMetadata(modelGuid, engineId)).start();
        Thread.sleep(300);

        ratingEngineProxy.setScoringIteration(mainCustomerSpace, engineId, model.getId(),
                BucketMetadataUtils.getDefaultMetadata(), null);
        return ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
    }

    private RatingEngine createCustomEventEngine(MetadataSegment segment, ModelSummary modelSummary)
            throws InterruptedException {
        RatingEngine ratingEngine = constructRatingEngine(RatingEngineType.CUSTOM_EVENT, segment);

        RatingEngine newEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);
        newEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
        assertNotNull(newEngine);
        Assert.assertNotNull(newEngine.getActiveModel(), JsonUtils.pprint(newEngine));
        log.info("Created rating engine " + newEngine.getId());

        AIModel model = (AIModel) newEngine.getActiveModel();
        configureCustomEventModel(model, "SomeFileName", CustomEventModelingType.CDL);
        model.setModelSummaryId(modelSummary.getId());

        ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), newEngine.getId(), model.getId(), model);
        log.info("Updated rating model " + model.getId());

        final String modelGuid = modelSummary.getId();
        final String engineId = newEngine.getId();
        ratingEngineProxy.setScoringIteration(mainCustomerSpace, engineId, model.getId(),
                BucketMetadataUtils.getDefaultMetadata(), null);
        new Thread(() -> insertBucketMetadata(modelGuid, engineId)).start();
        Thread.sleep(300);
        return ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
    }

    private void insertBucketMetadata(String modelGuid, String engineId) {
        CreateBucketMetadataRequest request = new CreateBucketMetadataRequest();
        request.setModelGuid(modelGuid);
        request.setRatingEngineId(engineId);
        request.setBucketMetadataList(getModifiedBucketMetadata());
        request.setLastModifiedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        bucketedScoreProxy.createABCDBuckets(mainTestTenant.getId(), request);
    }

    private void verifyProcess() {
        refreshRatingEngines();
        runCommonPAVerifications();
        verifyStats(false, BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.PurchaseHistory,
                BusinessEntity.Rating);
        verifyRuleBasedEngines();
        verifyDecoratedMetadata();
        if (ENABLE_AI_RATINGS) {
            verifyBucketMetadata(ai1.getId());
            verifyBucketMetadata(ai2.getId());
            verifyPublishedIterations(ai1);
            verifyPublishedIterations(ai2);
        }

    }

    private void refreshRatingEngines() {
        rule1 = ratingEngineProxy.getRatingEngine(mainCustomerSpace, rule1.getId());
        rule2 = ratingEngineProxy.getRatingEngine(mainCustomerSpace, rule2.getId());
        rule3 = ratingEngineProxy.getRatingEngine(mainCustomerSpace, rule3.getId());
        ai1 = ratingEngineProxy.getRatingEngine(mainCustomerSpace, ai1.getId());
        ai2 = ratingEngineProxy.getRatingEngine(mainCustomerSpace, ai2.getId());
    }

    private void verifyPublishedIterations(RatingEngine ratingEngine) {
        RatingEngine re = ratingEngineProxy.getRatingEngine(mainCustomerSpace, ratingEngine.getId());
        Assert.assertNotNull(re.getPublishedIteration());
        Assert.assertEquals(re.getPublishedIteration().getId(), ratingEngine.getScoringIteration().getId());
    }

    private void verifyRuleBasedEngines() {
        Map<RatingBucketName, Long> ratingCounts = ImmutableMap.of( //
                RatingBucketName.A, 4L, //
                RatingBucketName.D, 9L);
        verifyRatingEngineCount(rule1.getId(), ratingCounts);
        verifyRatingEngineCount(rule2.getId(), ratingCounts);
        verifyPublishedIterations(rule1);
        verifyPublishedIterations(rule2);
    }

    private void verifyDecoratedMetadata() {
        List<ColumnMetadata> ratingMetadata = getFullyDecoratedMetadata(BusinessEntity.Rating);
        log.info("Rating attrs: "
                + ratingMetadata.stream().map(ColumnMetadata::getAttrName).collect(Collectors.toList()));
        // Assert.assertEquals(ratingMetadata.size(), 12,
        // JsonUtils.serialize(ratingMetadata));
    }

    private ProcessAnalyzeRequest constructRequest() {
        ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
        request.setRebuildEntities(Collections.singleton(BusinessEntity.Rating));
        request.setMaxRatingIterations(2);
        return request;
    }

    private List<BucketMetadata> getModifiedBucketMetadata() {
        List<BucketMetadata> buckets = new ArrayList<>();
        buckets.add(BucketMetadataUtils.bucket(99, 90, BucketName.A));
        buckets.add(BucketMetadataUtils.bucket(90, 85, BucketName.B));
        buckets.add(BucketMetadataUtils.bucket(85, 40, BucketName.C));
        buckets.add(BucketMetadataUtils.bucket(40, 5, BucketName.D));
        long currentTime = System.currentTimeMillis();
        buckets.forEach(bkt -> bkt.setCreationTimestamp(currentTime));
        return buckets;
    }

    private void verifyBucketMetadata(String engineId) {
        log.info("Verifying bucket metadata for engine " + engineId);
        Map<Long, List<BucketMetadata>> bucketMetadataHistory = bucketedScoreProxy
                .getABCDBucketsByEngineId(mainTestTenant.getId(), engineId);
        Assert.assertNotNull(bucketMetadataHistory);
        Assert.assertEquals(bucketMetadataHistory.size(), 3);
        log.info("time is " + bucketMetadataHistory.keySet().toString());
        List<BucketMetadata> latestBucketedMetadata = bucketedScoreProxy
                .getLatestABCDBucketsByEngineId(mainTestTenant.getId(), engineId);
        latestBucketedMetadata.forEach(bucket -> Assert.assertEquals(bucket.getPublishedVersion().intValue(), 0));
        log.info("bucket metadata is " + JsonUtils.serialize(latestBucketedMetadata));
    }
}
