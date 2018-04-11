package com.latticeengines.apps.cdl.end2end.dataingestion;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.CrossSellModelingConfigKeys;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
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
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.testframework.exposed.proxy.pls.ModelSummaryProxy;

/**
 * This test is for generating model artifacts for other tests
 */
public class CrossSellModelEnd2EndDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CrossSellModelEnd2EndDeploymentTestNG.class);
    private static final boolean USE_EXISTING_TENANT = false;
    private static final String EXISTING_TENANT = "JLM1523408085423";

    private static final PredictionType PREDICTION_TYPE = PredictionType.EXPECTED_VALUE;

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

    @Value("${common.test.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceProxy;

    @Inject
    private BucketedScoreProxy bucketedScoreProxy;

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
        setupBusinessCalendar();
        setupTestRatingEngine();
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
    }

    @Test(groups = "end2end")
    public void runTest() {
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
        Map<Long, List<BucketMetadata>> bucketMetadataHistory = internalResourceProxy
                .getABCDBucketsBasedOnRatingEngineId(CustomerSpace.parse(mainTestTenant.getId()).toString(),
                        testRatingEngine.getId());
        Assert.assertNotNull(bucketMetadataHistory);
        Assert.assertEquals(bucketMetadataHistory.size(), 1);
        log.info("time is " + bucketMetadataHistory.keySet().toString());
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
        testAIModel.setPredictionType(PREDICTION_TYPE);
        config.setTargetProducts(Collections.singletonList(targetProductId));
        config.setTrainingProducts(Collections.singletonList(trainingProductId));
        testAIModel.setTrainingSegment(trainSegment);

        testAIModel = (AIModel) ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), testRatingEngine.getId(),
                testAIModel.getId(), testAIModel);

        long targetCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), ModelingQueryType.TARGET);
        Assert.assertEquals(targetCount, 22);

        long trainingCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), ModelingQueryType.TRAINING);
        Assert.assertEquals(trainingCount, 581);

        long eventCount = ratingEngineProxy.getModelingQueryCountByRatingId(mainTestTenant.getId(),
                testRatingEngine.getId(), testAIModel.getId(), ModelingQueryType.EVENT);
        Assert.assertEquals(eventCount, 56);

        log.info("Created: ratingengines/" + testRatingEngine.getId() + "/ratingmodels/" + testAIModel.getId());
    }

    @Override
    protected MetadataSegment constructTargetSegment() {
        Bucket stateBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList("No"));
        BucketRestriction accountRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "OUT_OF_BUSINESS_INDICATOR"), stateBkt);

        String productId = "1E26CD1E01559048FF7B51ADA27EA7AB";
        AggregationFilter spendFilter = new AggregationFilter(AggregationSelector.SPENT, AggregationType.SUM,
                ComparisonType.GREATER_THAN, Collections.singletonList(1));
        AggregationFilter unitFilter = new AggregationFilter(AggregationSelector.UNIT, AggregationType.SUM,
                ComparisonType.GREATER_THAN, Collections.singletonList(1));

        TransactionRestriction trxRes = new TransactionRestriction(productId, new TimeFilter(ComparisonType.BEFORE,
                PeriodStrategy.Template.Date.name(), Collections.singletonList("2018-04-09")), false, spendFilter,
                unitFilter);

        Bucket.Transaction txn = new Bucket.Transaction(productId, new TimeFilter(ComparisonType.BETWEEN_DATE,
                PeriodStrategy.Template.Date.name(), Arrays.asList("2017-04-09", "2018-04-09")), spendFilter,
                unitFilter, false);
        Bucket bkt = Bucket.txnBkt(txn);
        BucketRestriction bktRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Product, productId),
                bkt);

        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_MODELING);
        segment.setDisplayName("End2End Segment Modeling");
        segment.setDescription("A test segment for CDL end2end modeling test.");
        segment.setAccountFrontEndRestriction(
                new FrontEndRestriction(Restriction.builder().and(accountRestriction, trxRes).build()));
        segment.setAccountRestriction(Restriction.builder().and(accountRestriction, trxRes, bktRestriction).build());
        return segment;
    }

    private void setupBusinessCalendar() {
        periodProxy.saveBusinessCalendar(mainTestTenant.getId(), getStartingDateBusinessCalendderForTest());
    }
}
