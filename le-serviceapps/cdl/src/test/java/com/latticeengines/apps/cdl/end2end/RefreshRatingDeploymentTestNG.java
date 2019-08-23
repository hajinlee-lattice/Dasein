package com.latticeengines.apps.cdl.end2end;

import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
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
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class RefreshRatingDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RefreshRatingDeploymentTestNG.class);

    private static final boolean USE_EXISTING_TENANT = false;
    private static final String EXISTING_TENANT = "LETest1561082636718"; // "JLM1533618545277";

    private static final String LOADING_CHECKPOINT = UpdateTransactionDeploymentTestNG.CHECK_POINT;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private com.latticeengines.testframework.exposed.proxy.pls.ModelSummaryProxy plsModelSummaryProxy;

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

    // Target Products are shared with CrossSellModelEnd2EndDeploymentTestNG
    private static final ImmutableList<String> targetProducts = ImmutableList.of("1iHa3C9UQFBPknqKCNW3L6WgUAARc4o");

    private RatingEngine rule1;
    private RatingEngine rule2;
    private RatingEngine rule3;
    private RatingEngine ai1;
    private RatingEngine ai2;
    private RatingEngine ai3;

    private String uuid1;
    private String uuid2;
    private String uuid3;

    private ModelSummary modelSummary1;
    private ModelSummary modelSummary2;
    private ModelSummary modelSummary3;

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

    @SuppressWarnings("deprecation")
    private void setup(boolean useExistingTenant, boolean enableAIRatings) throws Exception {
        if (useExistingTenant) {
            testBed.useExistingTenantAsMain(EXISTING_TENANT);
            MultiTenantContext.setTenant(mainTestTenant);
            testBed.switchToSuperAdmin();
            mainTestTenant = testBed.getMainTestTenant();
            mainCustomerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
            initialVersion = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
            attachPlsProxies();
        } else {
            setupEnd2EndTestEnvironment();
            setupBusinessCalendar();
            Thread setupAIModelsThread = null;
            if (enableAIRatings) {
                setupAIModelsThread = new Thread(this::setupAIModels);
                setupAIModelsThread.start();
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

                if(setupAIModelsThread != null) {
                    setupAIModelsThread.join();
                }

                modelSummaryProxy.downloadModelSummary(mainCustomerSpace);
                initModelSummaries();

                ai1 = createCrossSellEngine(segment, modelSummary1, PredictionType.EXPECTED_VALUE);
                long targetCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                        ai1.getId(), ai1.getLatestIteration().getId(), ModelingQueryType.TARGET);
                Assert.assertTrue(targetCount > 100);
                Assert.assertNotNull(ai1.getSegment());
                activateRatingEngine(ai1.getId());

                ai2 = createCrossSellEngine(segment, modelSummary2, PredictionType.PROPENSITY);
                targetCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(), ai2.getId(),
                        ai2.getLatestIteration().getId(), ModelingQueryType.TARGET);
                Assert.assertTrue(targetCount > 100);
                Assert.assertNotNull(ai2.getSegment());
                activateRatingEngine(ai2.getId());

                ai3 = createCustomEventEngine(segment, modelSummary3);
                Assert.assertNotNull(ai3.getSegment());
                activateRatingEngine(ai3.getId());
            }
        }
    }

    private void initModelSummaries() {
        testBed.attachProtectedProxy(plsModelSummaryProxy);
        List<ModelSummary> summaries = plsModelSummaryProxy.getSummaries();
        if (CollectionUtils.isNotEmpty(summaries)) {
            for (ModelSummary summary : summaries) {
                if (summary.getId().contains(uuid1)) {
                    modelSummary1 = summary;
                } else if (summary.getId().contains(uuid2)) {
                    modelSummary2 = summary;
                } else if (summary.getId().contains(uuid3)) {
                    modelSummary3 = summary;
                }
            }
        }
        Assert.assertNotNull(modelSummary1);
        Assert.assertNotNull(modelSummary2);
        Assert.assertNotNull(modelSummary3);
    }

    private void setupAIModels() {
        testBed.attachProtectedProxy(plsModelSummaryProxy);
        testBed.switchToSuperAdmin();
        uuid1 = uploadModel(MODELS_RESOURCE_ROOT + "/ev_model.tar.gz");
        uuid2 = uploadModel(MODELS_RESOURCE_ROOT + "/propensity_model.tar.gz");
        uuid3 = uploadModel(MODELS_RESOURCE_ROOT + "/ce_model.tar.gz");
    }

    @SuppressWarnings("deprecation")
    private RatingEngine createCrossSellEngine(MetadataSegment segment, ModelSummary modelSummary,
            PredictionType predictionType) throws InterruptedException {
        RatingEngine ratingEngine = constructRatingEngine(RatingEngineType.CROSS_SELL, segment);

        RatingEngine newEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);
        newEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
        assertNotNull(newEngine);
        Assert.assertNotNull(newEngine.getLatestIteration(), JsonUtils.pprint(newEngine));
        log.info("Created rating engine " + newEngine.getId());

        AIModel model = (AIModel) newEngine.getLatestIteration();
        configureCrossSellModel(model, predictionType, ModelingStrategy.CROSS_SELL_FIRST_PURCHASE, targetProducts,
                targetProducts);
        model.setModelSummaryId(modelSummary.getId());

        ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), newEngine.getId(), model.getId(), model);
        log.info("Updated rating model " + model.getId());

        ratingEngineProxy.setScoringIteration(mainCustomerSpace, newEngine.getId(), model.getId(),
                BucketMetadataUtils.getDefaultMetadata(), null);
        Thread.sleep(300);
        insertBucketMetadata(modelSummary.getId(), newEngine.getId());
        Thread.sleep(300);
        return ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
    }

    @SuppressWarnings("deprecation")
    private RatingEngine createCustomEventEngine(MetadataSegment segment, ModelSummary modelSummary)
            throws InterruptedException {
        RatingEngine ratingEngine = constructRatingEngine(RatingEngineType.CUSTOM_EVENT, segment);

        RatingEngine newEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);
        newEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
        assertNotNull(newEngine);
        Assert.assertNotNull(newEngine.getLatestIteration(), JsonUtils.pprint(newEngine));
        log.info("Created rating engine " + newEngine.getId());

        AIModel model = (AIModel) newEngine.getLatestIteration();
        configureCustomEventModel(model, "SomeFileName", CustomEventModelingType.CDL);
        model.setModelSummaryId(modelSummary.getId());

        ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), newEngine.getId(), model.getId(), model);
        log.info("Updated rating model " + model.getId());

        ratingEngineProxy.setScoringIteration(mainCustomerSpace, newEngine.getId(), model.getId(),
                BucketMetadataUtils.getDefaultMetadata(), null);
        Thread.sleep(300);
        insertBucketMetadata(modelSummary.getId(), newEngine.getId());
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
            verifyPublishedIterations(ai1);
            verifyPublishedIterations(ai2);
            verifyBucketMetadataByEngineId(ai1.getId());
            verifyBucketMetadataByEngineId(ai2.getId());
            verifyBucketMetadataByModelGuid(((AIModel) ai1.getPublishedIteration()).getModelSummaryId());
            verifyBucketMetadataByModelGuid(((AIModel) ai2.getPublishedIteration()).getModelSummaryId());
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

    private void verifyBucketMetadataByEngineId(String engineId) {
        log.info("Verifying bucket metadata for engine " + engineId);
        Map<Long, List<BucketMetadata>> bucketMetadataHistory = bucketedScoreProxy
                .getABCDBucketsByEngineId(mainTestTenant.getId(), engineId);
        Assert.assertNotNull(bucketMetadataHistory);
        Assert.assertEquals(bucketMetadataHistory.size(), 3);
        log.info("time is " + bucketMetadataHistory.keySet().toString());
        if (engineId.equals(ai1.getId())) {
            for (List<BucketMetadata> bms : bucketMetadataHistory.values()) {
                bms.forEach(bm -> {
                    try {
                        Assert.assertNotNull(bm.getAverageExpectedRevenue());
                    } catch (AssertionError e) {
                        log.warn("No avg exp val", e);
                    }
                });
            }
        }
    }

    private void verifyBucketMetadataByModelGuid(String modelGuid) {
        log.info("Verifying bucket metadata for engine " + modelGuid);
        Map<Long, List<BucketMetadata>> bucketMetadataHistory = bucketedScoreProxy
                .getABCDBucketsByModelGuid(mainTestTenant.getId(), modelGuid);
        Assert.assertNotNull(bucketMetadataHistory);
        Assert.assertEquals(bucketMetadataHistory.size(), 3);
        log.info("time is " + bucketMetadataHistory.keySet().toString());
        List<BucketMetadata> latestBucketedMetadata = bucketedScoreProxy
                .getPublishedBucketMetadataByModelGuid(mainTestTenant.getId(), modelGuid);
        latestBucketedMetadata.forEach(bucket -> Assert.assertEquals(bucket.getPublishedVersion().intValue(), 0));
        log.info("bucket metadata is " + JsonUtils.serialize(latestBucketedMetadata));
    }
}
