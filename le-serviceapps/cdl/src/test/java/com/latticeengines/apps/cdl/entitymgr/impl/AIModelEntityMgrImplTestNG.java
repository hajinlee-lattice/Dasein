package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
    private static final String UPDATED_BY = "lattice@lattice-engines.com";

    private static final String PRODUCT_ID1 = "PID1";
    private static final String PRODUCT_ID2 = "PID2";
    private static final String PRODUCT_ID3 = "PID3";

    private static final String APP_JOB_ID = "application_1510227628013_17833";

    @Autowired
    private AIModelEntityMgr aiModelEntityMgr;

    @Autowired
    private RatingEngineEntityMgr ratingEngineEntityMgr;

    private RatingEngine crossSellratingEngine;
    private String crossSellRatingEngineId;

    private AIModel crossSellAIModel;
    private String crossSellAIModelId;

    private RatingEngine customEventRatingEngine;
    private String customEventRatingEngineId;

    private AIModel customEventAIModel;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithDummySegment();
        crossSellratingEngine = new RatingEngine();
        crossSellratingEngine.setDisplayName(RATING_ENGINE_NAME);
        crossSellratingEngine.setNote(RATING_ENGINE_NOTE);
        crossSellratingEngine.setSegment(testSegment);
        crossSellratingEngine.setCreatedBy(CREATED_BY);
        crossSellratingEngine.setUpdatedBy(UPDATED_BY);
        crossSellratingEngine.setType(RatingEngineType.CROSS_SELL);

        crossSellratingEngine.setId(UUID.randomUUID().toString());
        RatingEngine createdRatingEngine = ratingEngineEntityMgr.createRatingEngine(crossSellratingEngine);
        Assert.assertNotNull(createdRatingEngine);
        crossSellRatingEngineId = createdRatingEngine.getId();
        createdRatingEngine = ratingEngineEntityMgr.findById(createdRatingEngine.getId(), true);
        Assert.assertNotNull(createdRatingEngine);
        Assert.assertNotNull(createdRatingEngine.getLatestIteration());
    }

    @Test(groups = "functional")
    public void testBasicOperations() {
        log.debug("Testing basic operations");
        RatingEngine createdRatingEngine = ratingEngineEntityMgr.findById(crossSellRatingEngineId);
        Assert.assertNotNull(createdRatingEngine);
        List<AIModel> aiModelList = aiModelEntityMgr.findAllByRatingEngineId(crossSellRatingEngineId, null);
        Assert.assertNotNull(aiModelList);
        Assert.assertEquals(aiModelList.size(), 1);
        crossSellAIModel = aiModelList.get(0);
        assertDefaultAIModel(crossSellAIModel);

        crossSellAIModelId = crossSellAIModel.getId();
        crossSellAIModel = aiModelEntityMgr.findById(crossSellAIModelId);
        assertDefaultAIModel(crossSellAIModel);
        Assert.assertEquals(crossSellAIModel.getRatingEngine().getId(), crossSellRatingEngineId);

        // update aiModel by updating its selected attributes and rules
        CrossSellModelingConfig.getAdvancedModelingConfig(crossSellAIModel)
                .setTargetProducts(generateSeletedProducts());
        crossSellAIModel = aiModelEntityMgr.updateAIModel(crossSellAIModel,
                aiModelEntityMgr.findById(crossSellAIModel.getId()), crossSellRatingEngineId);

        aiModelList = aiModelEntityMgr.findAllByRatingEngineId(crossSellRatingEngineId, null);

        Assert.assertNotNull(aiModelList);
        Assert.assertEquals(aiModelList.size(), 1);
        crossSellAIModel = aiModelEntityMgr.findById(aiModelList.get(0).getId());

        assertUpdatedAIModel(crossSellAIModel);
    }

    @Test(groups = "functional", dependsOnMethods = { "testBasicOperations" })
    public void testUpdateTrainingData() {
        crossSellAIModel.setTrainingSegment(createMetadataSegment(TRAINING_SEGMENT_NAME));
        CrossSellModelingConfig.getAdvancedModelingConfig(crossSellAIModel)
                .setTrainingProducts(generateTrainingProducts());

        crossSellAIModel.setModelingJobId(APP_JOB_ID);
        aiModelEntityMgr.updateAIModel(crossSellAIModel, aiModelEntityMgr.findById(crossSellAIModel.getId()),
                crossSellRatingEngineId);

        crossSellAIModel = aiModelEntityMgr.findById(crossSellAIModel.getId());
        Assert.assertNotNull(crossSellAIModel, "Could not find AIModel");
        assertUpdatedModelWithTrainingData(crossSellAIModel);
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
        CrossSellModelingConfig.getAdvancedModelingConfig(crossSellAIModel).setFilters(configFitlers);

        aiModelEntityMgr.updateAIModel(crossSellAIModel, aiModelEntityMgr.findById(crossSellAIModel.getId()),
                crossSellRatingEngineId);

        crossSellAIModel = aiModelEntityMgr.findById(crossSellAIModel.getId());
        assertUpdatedModelWithConfigFilters(crossSellAIModel, configFitlers);
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateRefineSettings" })
    public void testUpdateSegmentNull() {
        crossSellratingEngine.setSegment(null);
        ratingEngineEntityMgr.updateRatingEngine(crossSellratingEngine,
                ratingEngineEntityMgr.findById(crossSellratingEngine.getId(), true), false);
        crossSellratingEngine = ratingEngineEntityMgr.findById(crossSellRatingEngineId);
        Assert.assertNotNull(crossSellratingEngine);
        Assert.assertNotNull(crossSellratingEngine.getSegment());

        crossSellratingEngine.setSegment(null);
        ratingEngineEntityMgr.updateRatingEngine(crossSellratingEngine,
                ratingEngineEntityMgr.findById(crossSellratingEngine.getId(), true), true);
        crossSellratingEngine = ratingEngineEntityMgr.findById(crossSellRatingEngineId);
        Assert.assertNotNull(crossSellratingEngine);
        Assert.assertNotNull(crossSellratingEngine.getSegment());
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateSegmentNull" })
    public void testCreateIteration() {
        AIModel iteration = new AIModel();
        RatingEngine createdRatingEngine = ratingEngineEntityMgr.findById(crossSellRatingEngineId);

        iteration.setRatingEngine(createdRatingEngine);
        iteration.setAdvancedModelingConfig(new CrossSellModelingConfig());
        iteration.setId(UUID.randomUUID().toString());
        iteration.setDerivedFromRatingModel(createdRatingEngine.getLatestIteration().getId());

        iteration = aiModelEntityMgr.createAIModel(iteration, crossSellRatingEngineId);
        Assert.assertNotNull(iteration);
        List<AIModel> aiModelList = aiModelEntityMgr.findAllByRatingEngineId(crossSellRatingEngineId, null);
        Assert.assertNotNull(aiModelList);
        Assert.assertEquals(aiModelList.size(), 2);

        crossSellAIModelId = crossSellAIModel.getId();
        crossSellAIModel = aiModelEntityMgr.findById(crossSellAIModelId);
        Assert.assertEquals(crossSellAIModel.getRatingEngine().getId(), crossSellRatingEngineId);
        Assert.assertEquals(iteration.getRatingEngine().getId(), crossSellRatingEngineId);
        assertCreatedAIModel(iteration);

        createdRatingEngine.setLatestIteration(iteration);
        createdRatingEngine = ratingEngineEntityMgr.updateRatingEngine(createdRatingEngine,
                ratingEngineEntityMgr.findById(crossSellRatingEngineId), false);

        createdRatingEngine = ratingEngineEntityMgr.findById(crossSellRatingEngineId);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateIteration" })
    public void testDelete() {
        ratingEngineEntityMgr.deleteById(crossSellratingEngine.getId(), true, CREATED_BY);
        crossSellAIModel = aiModelEntityMgr.findById(crossSellAIModel.getId());
        Assert.assertNull(crossSellAIModel, "AIModel is not deleted");
    }

    @Test(groups = "functional", dependsOnMethods = { "testDelete" })
    public void testCreateCustomEvent() throws Exception {
        customEventRatingEngine = new RatingEngine();
        customEventRatingEngine.setDisplayName(RATING_ENGINE_NAME);
        customEventRatingEngine.setNote(RATING_ENGINE_NOTE);
        customEventRatingEngine.setSegment(testSegment);
        customEventRatingEngine.setCreatedBy(CREATED_BY);
        customEventRatingEngine.setUpdatedBy(UPDATED_BY);
        customEventRatingEngine.setType(RatingEngineType.CUSTOM_EVENT);

        customEventRatingEngine.setId(UUID.randomUUID().toString());
        customEventRatingEngine = ratingEngineEntityMgr.createRatingEngine(customEventRatingEngine);
        Assert.assertNotNull(customEventRatingEngine);
        customEventRatingEngineId = customEventRatingEngine.getId();
        customEventRatingEngine = ratingEngineEntityMgr.findById(customEventRatingEngine.getId(), true);
        Assert.assertNotNull(customEventRatingEngine);
        Assert.assertNotNull(customEventRatingEngine.getLatestIteration());
        Assert.assertNotNull(customEventRatingEngine.getAdvancedRatingConfig());
        Assert.assertEquals(customEventRatingEngine.getAdvancedRatingConfig().getClass(),
                CustomEventRatingConfig.class);

        customEventRatingEngine = ratingEngineEntityMgr.findById(customEventRatingEngineId, true);
        Assert.assertNotNull(customEventRatingEngine);
        List<AIModel> aiModelList = aiModelEntityMgr.findAllByRatingEngineId(customEventRatingEngineId, null);
        Assert.assertNotNull(aiModelList);
        Assert.assertEquals(aiModelList.size(), 1);
        customEventAIModel = aiModelList.get(0);

        Assert.assertNotNull(customEventAIModel);
        Assert.assertNotNull(customEventAIModel.getId());
        Assert.assertEquals(customEventAIModel.getIteration(), 1);
        Assert.assertNotNull(customEventAIModel.getRatingEngine());
        Assert.assertNotNull(customEventAIModel.getAdvancedModelingConfig());
        Assert.assertEquals(customEventAIModel.getAdvancedModelingConfig().getClass(), CustomEventModelingConfig.class);

        Assert.assertNotNull(customEventRatingEngine.getLatestIteration().getId());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateCustomEvent" })
    public void testUpdateSegmentNullCustomEvent() {
        customEventRatingEngine.setSegment(null);
        ratingEngineEntityMgr.updateRatingEngine(customEventRatingEngine,
                ratingEngineEntityMgr.findById(customEventRatingEngine.getId(), true), false);
        customEventRatingEngine = ratingEngineEntityMgr.findById(customEventRatingEngineId);
        Assert.assertNotNull(customEventRatingEngine);
        Assert.assertNotNull(customEventRatingEngine.getSegment());

        customEventRatingEngine.setSegment(null);
        ratingEngineEntityMgr.updateRatingEngine(customEventRatingEngine,
                ratingEngineEntityMgr.findById(customEventRatingEngine.getId(), true), true);
        customEventRatingEngine = ratingEngineEntityMgr.findById(customEventRatingEngineId);
        Assert.assertNotNull(customEventRatingEngine);
        Assert.assertNull(customEventRatingEngine.getSegment());
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateSegmentNullCustomEvent" })
    public void testDeleteCustomEvent() {
        ratingEngineEntityMgr.deleteById(customEventRatingEngine.getId(), true, CREATED_BY);
        customEventAIModel = aiModelEntityMgr.findById(customEventAIModel.getId());
        Assert.assertNull(customEventAIModel, "AIModel is not deleted");
    }

    private void assertCreatedAIModel(AIModel aiModel) {
        Assert.assertNotNull(aiModel);
        Assert.assertEquals(2, aiModel.getIteration());
        Assert.assertNotNull(aiModel.getRatingEngine());
        Assert.assertEquals(aiModel.getRatingEngine().getId(), crossSellRatingEngineId);
    }

    private void assertUpdatedAIModel(AIModel aiModel) {
        Assert.assertNotNull(aiModel);
        Assert.assertEquals(aiModel.getId(), crossSellAIModelId);
        Assert.assertEquals(1, aiModel.getIteration());
        Assert.assertNotNull(aiModel.getRatingEngine());
        Assert.assertEquals(aiModel.getRatingEngine().getId(), crossSellRatingEngineId);

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
