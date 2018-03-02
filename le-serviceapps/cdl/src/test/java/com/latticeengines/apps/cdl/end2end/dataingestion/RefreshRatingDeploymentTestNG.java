package com.latticeengines.apps.cdl.end2end.dataingestion;

import static org.testng.Assert.assertNotNull;

import java.util.Collections;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.testframework.exposed.proxy.pls.ModelSummaryProxy;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class RefreshRatingDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RefreshRatingDeploymentTestNG.class);

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private ModelSummaryProxy modelSummaryProxy;

    @Inject
    private SegmentProxy segmentProxy;

    @Value("${camille.zk.pod.id}")
    private String podId;

    private static final String MODELS_RESOURCE_ROOT = "end2end/models";
    private static final boolean ENABLE_AI_RATINGS = true;

    private RatingEngine rule1;
    private RatingEngine rule2;
    private RatingEngine ai1;
    private RatingEngine ai2;

    private String uuid1;
    private String uuid2;

    @Test(groups = "end2end")
    public void runTest() throws Exception {
        if (ENABLE_AI_RATINGS) {
            new Thread(this::setupAIModels).start();
        }

        resumeVdbCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        verifyStats(BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.PurchaseHistory);

        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));

        createModelingSegment();
        MetadataSegment segment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(), SEGMENT_NAME_MODELING);
        Assert.assertNotNull(segment);

        new Thread(() -> {
            createTestSegment2();
            rule1 = createRuleBasedRatingEngine();
            rule2 = createRuleBasedRatingEngine();
        }).start();

        if (ENABLE_AI_RATINGS) {
            ModelSummary modelSummary = waitToDownloadModelSummaryWithUuid(modelSummaryProxy, uuid1);
            ai1 = createAIEngine(segment, modelSummary, PredictionType.EXPECTED_VALUE);
            modelSummary = waitToDownloadModelSummaryWithUuid(modelSummaryProxy, uuid2);
            ai2 = createAIEngine(segment, modelSummary, PredictionType.PROPENSITY);
        }

        processAnalyze(constructRequest());
        verifyProcess();
    }

    private void setupAIModels() {
        testBed.attachProtectedProxy(modelSummaryProxy);
        testBed.switchToSuperAdmin();
        uuid1 = uploadModel(MODELS_RESOURCE_ROOT + "/ev_model.tar.gz");
        uuid2 = uploadModel(MODELS_RESOURCE_ROOT + "/prop_model.tar.gz");
    }

    private RatingEngine createAIEngine(MetadataSegment segment, ModelSummary modelSummary, PredictionType predictionType) throws InterruptedException {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setCreatedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        ratingEngine.setSegment(segment);
        ratingEngine.setDisplayName("CDL End2End AI Engine");
        ratingEngine.setType(RatingEngineType.CROSS_SELL);

        RatingEngine newEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);
        newEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
        assertNotNull(newEngine);
        Assert.assertNotNull(newEngine.getActiveModel(), JsonUtils.pprint(newEngine));

        AIModel model = createAIModel((AIModel) newEngine.getActiveModel(), modelSummary, predictionType);
        ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), newEngine.getId(), model.getId(), model);
        Thread.sleep(300);
        return ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
    }

    private AIModel createAIModel(AIModel aiModel, ModelSummary modelSummary, PredictionType predictionType) {
        aiModel.setModelSummary(modelSummary);
        aiModel.setTargetProducts(Collections.singletonList(TARGET_PRODUCT));
        aiModel.setPredictionType(predictionType);
        aiModel.setModelingStrategy(ModelingStrategy.CROSS_SELL_FIRST_PURCHASE);
        return aiModel;
    }

    private void verifyProcess() {
        runCommonPAVerifications();
        verifyStats(BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.PurchaseHistory,
                BusinessEntity.Rating);
        verifyRuleBasedEngines();
    }

    private void verifyRuleBasedEngines() {
        Map<RatingBucketName, Long> ratingCounts = ImmutableMap.of( //
                RatingBucketName.A, RATING_A_COUNT_1, //
                RatingBucketName.D, RATING_D_COUNT_1, //
                RatingBucketName.F, RATING_F_COUNT_1);
        verifyRatingEngineCount(rule1.getId(), ratingCounts);
        verifyRatingEngineCount(rule2.getId(), ratingCounts);
    }

    private ProcessAnalyzeRequest constructRequest() {
        ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
        request.setRebuildEntities(Collections.singleton(BusinessEntity.Rating));
        return request;
    }

}
