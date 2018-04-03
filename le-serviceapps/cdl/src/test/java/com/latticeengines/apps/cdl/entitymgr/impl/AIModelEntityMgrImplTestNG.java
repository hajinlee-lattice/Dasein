package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.AIModelEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.RatingEngineEntityMgr;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.CrossSellModelingConfigKeys;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.cdl.rating.CustomEventRatingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.query.ComparisonType;

public class AIModelEntityMgrImplTestNG extends CDLFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(AIModelEntityMgrImplTestNG.class);

    private static final String TRAINING_SEGMENT_NAME = "Training Segment Name";

    private static final String RATING_ENGINE_NAME = "Rating Engine for AI Model";
    private static final String RATING_ENGINE_NOTE = "This is a Rating Engine that covers North America market";
    private static final String CREATED_BY = "lattice@lattice-engines.com";

    private static final String PRODUCT_ID1 = "PID1";
    private static final String PRODUCT_ID2 = "PID2";
    private static final String PRODUCT_ID3 = "PID3";

    private static final String APP_JOB_ID = "application_1510227628013_17833";

    @Autowired
    private AIModelEntityMgr aiModelEntityMgr;

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    private RatingEngine ratingEngine;
    private String ratingEngineId;

    private AIModel aiModel;
    private String aiModelId;

    private RatingEngine ratingEngineCustomEvent;
    private String ratingEngineIdCustomEvent;

    private AIModel aiModelCustomEvent;
    private String aiModelIdCustomEvent;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithDummySegment();
        ratingEngine = new RatingEngine();
        ratingEngine.setDisplayName(RATING_ENGINE_NAME);
        ratingEngine.setNote(RATING_ENGINE_NOTE);
        ratingEngine.setSegment(testSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.CROSS_SELL);

        RatingEngine createdRatingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine,
                mainTestTenant.getId());
        Assert.assertNotNull(createdRatingEngine);
        ratingEngineId = createdRatingEngine.getId();
        createdRatingEngine = ratingEngineEntityMgr.findById(createdRatingEngine.getId(), true);
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getActiveModel());
    }

    @Test(groups = "functional")
    public void testBasicOperations() {
        log.debug("Testing basic operations");
        RatingEngine createdRatingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        Assert.assertNotNull(createdRatingEngine);
        List<AIModel> aiModelList = aiModelEntityMgr.findByRatingEngineId(ratingEngineId, null);
        Assert.assertNotNull(aiModelList);
        Assert.assertEquals(aiModelList.size(), 1);
        aiModel = aiModelList.get(0);
        assertDefaultAIModel(aiModel);

        aiModelId = aiModel.getId();
        aiModel = aiModelEntityMgr.findById(aiModelId);
        assertDefaultAIModel(aiModel);
        Assert.assertEquals(aiModel.getRatingEngine().getId(), ratingEngineId);

        // update aiModel by updating its selected attributes and rules
        CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).setTargetProducts(generateSeletedProducts());
        aiModel = aiModelEntityMgr.createOrUpdateAIModel(aiModel, ratingEngineId);

        aiModelList = aiModelEntityMgr.findByRatingEngineId(ratingEngineId, null);

        Assert.assertNotNull(aiModelList);
        Assert.assertEquals(aiModelList.size(), 1);
        aiModel = aiModelEntityMgr.findById(aiModelList.get(0).getId());

        assertUpdatedAIModel(aiModel);
    }

    @Test(groups = "functional", dependsOnMethods = { "testBasicOperations" })
    public void testUpdateTrainingData() {
        aiModel.setTrainingSegment(createMetadataSegment(TRAINING_SEGMENT_NAME));
        CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).setTrainingProducts(generateTrainingProducts());

        aiModel.setModelingJobId(APP_JOB_ID);
        aiModelEntityMgr.createOrUpdateAIModel(aiModel, ratingEngineId);

        aiModel = aiModelEntityMgr.findById(aiModel.getId());
        Assert.assertNotNull(aiModel, "Could not find AIModel");
        assertUpdatedModelWithTrainingData(aiModel);
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateTrainingData" })
    public void testUpdateRefineSettings() {
        ModelingConfigFilter spendFilter = new ModelingConfigFilter(CrossSellModelingConfigKeys.SPEND_IN_PERIOD,
                ComparisonType.LESS_OR_EQUAL, 1500);
        ModelingConfigFilter quantityFilter = new ModelingConfigFilter(CrossSellModelingConfigKeys.QUANTITY_IN_PERIOD,
                ComparisonType.GREATER_OR_EQUAL, 10);
        ModelingConfigFilter trainingFilter = new ModelingConfigFilter(CrossSellModelingConfigKeys.TRAINING_SET_PERIOD,
                ComparisonType.WITHIN, 10);
        ModelingConfigFilter repeatPurchaseFilter = new ModelingConfigFilter(
                CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD, ComparisonType.PRIOR, 6);

        Map<CrossSellModelingConfigKeys, ModelingConfigFilter> configFitlers = new HashMap<>();
        configFitlers.put(CrossSellModelingConfigKeys.SPEND_IN_PERIOD, spendFilter);
        configFitlers.put(CrossSellModelingConfigKeys.QUANTITY_IN_PERIOD, quantityFilter);
        configFitlers.put(CrossSellModelingConfigKeys.TRAINING_SET_PERIOD, trainingFilter);
        configFitlers.put(CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD, repeatPurchaseFilter);
        CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).setFilters(configFitlers);

        aiModelEntityMgr.createOrUpdateAIModel(aiModel, ratingEngineId);

        aiModel = aiModelEntityMgr.findById(aiModel.getId());
        assertUpdatedModelWithConfigFilters(aiModel, configFitlers);
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateRefineSettings" })
    public void testUpdateSegmentNull() {
        ratingEngine.setSegment(null);
        ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine, mainTestTenant.getId(), false);
        ratingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        Assert.assertNotNull(ratingEngine);
        Assert.assertNotNull(ratingEngine.getSegment());

        ratingEngine.setSegment(null);
        ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine, mainTestTenant.getId(), true);
        ratingEngine = ratingEngineEntityMgr.findById(ratingEngineId);
        Assert.assertNotNull(ratingEngine);
        Assert.assertNotNull(ratingEngine.getSegment());
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateSegmentNull" })
    public void testDelete() {
        aiModelEntityMgr.deleteById(aiModel.getId());
        aiModel = aiModelEntityMgr.findById(aiModel.getId());
        Assert.assertNull(aiModel, "AIModel is not deleted");
    }

    @Test(groups = "functional", dependsOnMethods = { "testDelete" })
    public void testCreateCustomEvent() throws Exception {
        ratingEngineCustomEvent = new RatingEngine();
        ratingEngineCustomEvent.setDisplayName(RATING_ENGINE_NAME);
        ratingEngineCustomEvent.setNote(RATING_ENGINE_NOTE);
        ratingEngineCustomEvent.setSegment(testSegment);
        ratingEngineCustomEvent.setCreatedBy(CREATED_BY);
        ratingEngineCustomEvent.setType(RatingEngineType.CUSTOM_EVENT);

        ratingEngineCustomEvent = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngineCustomEvent,
                mainTestTenant.getId());
        Assert.assertNotNull(ratingEngineCustomEvent);
        ratingEngineIdCustomEvent = ratingEngineCustomEvent.getId();
        ratingEngineCustomEvent = ratingEngineEntityMgr.findById(ratingEngineCustomEvent.getId(), true);
        Assert.assertNotNull(ratingEngineCustomEvent);
        Assert.assertNotNull(ratingEngineCustomEvent.getActiveModel());
        Assert.assertNotNull(ratingEngineCustomEvent.getAdvancedRatingConfig());
        Assert.assertEquals(ratingEngineCustomEvent.getAdvancedRatingConfig().getClass(),
                CustomEventRatingConfig.class);

        ratingEngineCustomEvent = ratingEngineEntityMgr.findById(ratingEngineIdCustomEvent, true);
        Assert.assertNotNull(ratingEngineCustomEvent);
        List<AIModel> aiModelList = aiModelEntityMgr.findByRatingEngineId(ratingEngineIdCustomEvent, null);
        Assert.assertNotNull(aiModelList);
        Assert.assertEquals(aiModelList.size(), 1);
        aiModelCustomEvent = aiModelList.get(0);

        Assert.assertNotNull(aiModelCustomEvent);
        Assert.assertNotNull(aiModelCustomEvent.getId());
        Assert.assertEquals(aiModelCustomEvent.getIteration(), 1);
        Assert.assertNotNull(aiModelCustomEvent.getRatingEngine());
        Assert.assertNotNull(aiModelCustomEvent.getAdvancedModelingConfig());
        Assert.assertEquals(aiModelCustomEvent.getAdvancedModelingConfig().getClass(), CustomEventModelingConfig.class);

        Assert.assertNotNull(ratingEngineCustomEvent.getActiveModel().getId());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateCustomEvent" })
    public void testUpdateSegmentNullCustomEvent() {
        ratingEngineCustomEvent.setSegment(null);
        ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngineCustomEvent, mainTestTenant.getId(), false);
        ratingEngineCustomEvent = ratingEngineEntityMgr.findById(ratingEngineIdCustomEvent);
        Assert.assertNotNull(ratingEngineCustomEvent);
        Assert.assertNotNull(ratingEngineCustomEvent.getSegment());

        ratingEngineCustomEvent.setSegment(null);
        ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngineCustomEvent, mainTestTenant.getId(), true);
        ratingEngineCustomEvent = ratingEngineEntityMgr.findById(ratingEngineIdCustomEvent);
        Assert.assertNotNull(ratingEngineCustomEvent);
        Assert.assertNull(ratingEngineCustomEvent.getSegment());
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateSegmentNullCustomEvent" })
    public void testDeleteCustomEvent() {
        aiModelEntityMgr.deleteById(aiModelCustomEvent.getId());
        aiModelCustomEvent = aiModelEntityMgr.findById(aiModelCustomEvent.getId());
        Assert.assertNull(aiModelCustomEvent, "AIModel is not deleted");
    }

    private void assertUpdatedAIModel(AIModel aiModel) {
        Assert.assertNotNull(aiModel);
        Assert.assertEquals(aiModel.getId(), aiModelId);
        Assert.assertEquals(1, aiModel.getIteration());
        Assert.assertNotNull(aiModel.getRatingEngine());
        Assert.assertEquals(aiModel.getRatingEngine().getId(), ratingEngineId);

        Assert.assertNotNull(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTargetProducts());
        Assert.assertTrue(
                CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTargetProducts().contains(PRODUCT_ID1));
        Assert.assertTrue(
                CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTargetProducts().contains(PRODUCT_ID2));
    }

    private void assertUpdatedModelWithTrainingData(AIModel aiModel) {
        assertUpdatedAIModel(aiModel);
        Assert.assertNotNull(aiModel.getTrainingSegment());
        Assert.assertNotNull(aiModel.getTrainingSegment().getName());

        Assert.assertNotNull(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTrainingProducts());
        Assert.assertNotNull(
                CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTrainingProducts().contains(PRODUCT_ID3));

        Assert.assertEquals(aiModel.getModelingYarnJobId().toString(), APP_JOB_ID);
    }

    private void assertUpdatedModelWithConfigFilters(AIModel aiModel,
            Map<CrossSellModelingConfigKeys, ModelingConfigFilter> filters) {
        assertUpdatedAIModel(aiModel);
        Assert.assertNotNull(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getFilters());
        Assert.assertEquals(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getFilters().size(),
                filters.size());
        for (ModelingConfigFilter filter : filters.values()) {
            Assert.assertTrue(
                    CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getFilters().values().contains(filter));
        }
    }

    private void assertDefaultAIModel(AIModel aiModel) {
        Assert.assertNotNull(aiModel);
        Assert.assertNotNull(aiModel.getId());
        Assert.assertEquals(aiModel.getIteration(), 1);
        Assert.assertNotNull(aiModel.getRatingEngine());

        Assert.assertTrue(CollectionUtils
                .isEmpty(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTargetProducts()));
    }

    private List<String> generateSeletedProducts() {
        List<String> selectedProducts = new ArrayList<>();
        selectedProducts.add(PRODUCT_ID1);
        selectedProducts.add(PRODUCT_ID2);
        return selectedProducts;
    }

    private List<String> generateTrainingProducts() {
        List<String> trainingProducts = new ArrayList<>();
        trainingProducts.add(PRODUCT_ID3);
        return trainingProducts;
    }
}
