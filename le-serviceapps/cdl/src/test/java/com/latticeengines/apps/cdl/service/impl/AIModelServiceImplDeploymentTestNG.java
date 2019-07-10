package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.AIModelService;
import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.CrossSellModelingConfigKeys;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.SegmentProxy;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.testframework.exposed.utils.ModelSummaryUtils;

public class AIModelServiceImplDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AIModelServiceImplDeploymentTestNG.class);

    private static final String CREATED_BY = "lattice@lattice-engines.com";

    private static final String TRAINING_SEGMENT_NAME = "Training Segment Name";

    private static final String PRODUCT_ID1 = "PID1";
    private static final String PRODUCT_ID2 = "PID2";
    private static final String PRODUCT_ID3 = "PID3";

    private static final String APP_JOB_ID = "application_1510227628013_17833";

    @Inject
    protected SegmentProxy segmentProxy;

    @Inject
    private RatingEngineService ratingEngineService;

    @Inject
    private AIModelService aiModelService;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    protected ModelSummaryProxy modelSummaryProxy;

    protected MetadataSegment reTestSegment;

    protected RatingEngine aiRatingEngine;
    protected AIModel iteration1;
    protected AIModel iteration2;
    protected String aiRatingEngineId;
    protected String aiRatingModelId;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId(), 3);
        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(),
                constructSegment(SEGMENT_NAME));
        Assert.assertNotNull(createdSegment);
        reTestSegment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(), createdSegment.getName());
        log.info(String.format("Created metadata segment with name %s", reTestSegment.getName()));

    }

    protected RatingEngine createRatingEngine(RatingEngine ratingEngine) {
        return ratingEngineService.createOrUpdate(ratingEngine);
    }

    protected RatingModel getRatingModel() {
        return ratingEngineService.getRatingModel(aiRatingEngineId, aiRatingModelId);
    }

    protected void updateRatingModel(AIModel aiModel) {
        ratingEngineService.updateRatingModel(aiRatingEngineId, aiRatingModelId, aiModel);
    }

    protected List<RatingEngineSummary> getAllRatingEngineSummaries() {
        return ratingEngineService.getAllRatingEngineSummaries();
    }

    protected RatingEngine getRatingEngineById(String ratingEngineId) {
        return ratingEngineService.getRatingEngineById(ratingEngineId, false);
    }

    protected void deleteRatingEngine(RatingEngine ratingEngine) {
        ratingEngineService.deleteById(ratingEngine.getId());
    }

    protected EventFrontEndQuery getRatingEngineModelingQueries(RatingEngine ratingEngine, AIModel aiModel,
            ModelingQueryType queryType) {
        return aiModelService.getModelingQuery(mainTestTenant.getId(), ratingEngine, aiModel, queryType, null);
    }

    @Test(groups = "deployment")
    public void testCreate() {
        aiRatingEngine = createTestRatingEngine(RatingEngineType.CROSS_SELL);
        Assert.assertEquals(aiRatingEngine.getType(), RatingEngineType.CROSS_SELL);
        aiRatingEngineId = aiRatingEngine.getId();

        List<RatingModel> ratingModels = ratingEngineService.getRatingModelsByRatingEngineId(aiRatingEngineId);
        Assert.assertEquals(ratingModels.size(), 1);
        aiRatingModelId = ratingModels.get(0).getId();
        Assert.assertNotNull(aiRatingModelId, "AIRatingModel is null");
    }

    @Test(groups = "deployment", dependsOnMethods = { "testCreate" })
    private void testFindAndUpdateRatingModelBasicFields() {
        // test update rating model
        AIModel aiModel = getSpecificRatingModel();
        assertDefaultAIModel(aiModel);

        CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).setTargetProducts(generateSelectedProducts());
        CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).setTrainingProducts(generateTrainingProducts());
        aiModel.setModelingJobId(APP_JOB_ID);

        updateRatingModel(aiModel);
        assertUpdatedAIModelWithBasicFields(getSpecificRatingModel());
    }

    @Test(groups = "deployment", dependsOnMethods = { "testFindAndUpdateRatingModelBasicFields" })
    private void testUpdateRatingModelRelationshipObjects() {
        AIModel aiModel = getSpecificRatingModel();

        MetadataSegment trainingSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(),
                constructSegment(TRAINING_SEGMENT_NAME));
        Assert.assertNotNull(trainingSegment);
        aiModel.setTrainingSegment(trainingSegment);

        updateRatingModel(aiModel);
        assertUpdatedModelWithRelationshipObjects(getSpecificRatingModel());
    }

    @Test(groups = "deployment", dependsOnMethods = { "testUpdateRatingModelRelationshipObjects" })
    private void testUpdateRatingModelConfigFilters() {
        // test get specific rating model
        AIModel aiModel = getSpecificRatingModel();

        ModelingConfigFilter spendFilter = new ModelingConfigFilter(CrossSellModelingConfigKeys.SPEND_IN_PERIOD,
                ComparisonType.LESS_OR_EQUAL, 1500);
        ModelingConfigFilter quantityFilter = new ModelingConfigFilter(CrossSellModelingConfigKeys.QUANTITY_IN_PERIOD,
                ComparisonType.LESS_OR_EQUAL, 10);
        ModelingConfigFilter trainFilter = new ModelingConfigFilter(CrossSellModelingConfigKeys.TRAINING_SET_PERIOD,
                ComparisonType.PRIOR_ONLY, 4);

        Map<CrossSellModelingConfigKeys, ModelingConfigFilter> configFilters = new HashMap<>();
        configFilters.put(CrossSellModelingConfigKeys.SPEND_IN_PERIOD, spendFilter);
        configFilters.put(CrossSellModelingConfigKeys.QUANTITY_IN_PERIOD, quantityFilter);
        configFilters.put(CrossSellModelingConfigKeys.TRAINING_SET_PERIOD, trainFilter);
        CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).setFilters(configFilters);

        updateRatingModel(aiModel);
        assertUpdatedModelWithConfigFilters(getSpecificRatingModel(), configFilters);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testUpdateRatingModelConfigFilters" })
    private void testGetModelingQueries() {
        AIModel aiModel = (AIModel) getRatingModel();
        EventFrontEndQuery targetQuery = getRatingEngineModelingQueries(aiRatingEngine, aiModel,
                ModelingQueryType.TARGET);
        EventFrontEndQuery trainingQuery = getRatingEngineModelingQueries(aiRatingEngine, aiModel,
                ModelingQueryType.TRAINING);
        EventFrontEndQuery eventQuery = getRatingEngineModelingQueries(aiRatingEngine, aiModel,
                ModelingQueryType.EVENT);

        assertModelingQueries(targetQuery, trainingQuery, eventQuery);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetModelingQueries" })
    private void testUpdateRatingModelWithModelSummary() {
        String uuid = UUID.randomUUID().toString();
        String applicationId = "application_1111111111111_1111";
        String modelVersion = "mver_" + uuid;
        String modelName = "model_" + uuid;
        String modelPath = "models/MulesoftAllRows20160314/";

        ModelSummaryUtils.TestModelConfiguration modelConfiguration = new ModelSummaryUtils.TestModelConfiguration(
                modelPath, modelName, applicationId, modelVersion, uuid);
        ModelSummary modelSummary = null;
        try {
            modelSummary = ModelSummaryUtils.createModelSummary(modelSummaryProxy, columnMetadataProxy, mainTestTenant,
                    modelConfiguration);
        } catch (IOException e) {
            Assert.fail("Could not create ModelSummary", e);
        }
        Assert.assertNotNull(modelSummary);
        log.info("Created ModelSummary ID: " + modelSummary.getId());

        ModelSummary retModelSummary = modelSummaryProxy.getModelSummaryFromModelId(mainTestTenant.getId(),
                modelConfiguration.getModelId());
        Assert.assertNotNull(retModelSummary);
        Assert.assertNotNull(retModelSummary.getId());

        AIModel aiModel = getSpecificRatingModel();

        ModelSummary selectedModelSummary = new ModelSummary();
        selectedModelSummary.setId(retModelSummary.getId());
        aiModel.setModelSummaryId(selectedModelSummary.getId());

        updateRatingModel(aiModel);
        assertUpdatedModelWithModelSummary(getSpecificRatingModel(), retModelSummary);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testUpdateRatingModelWithModelSummary" })
    private void testGetDependentAttrsInAllModels() {
        List<AttributeLookup> attributes = ratingEngineService.getDependentAttrsInAllModels(aiRatingEngineId);
        Assert.assertNotNull(attributes);
        Assert.assertTrue(attributes.size() > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetDependentAttrsInAllModels" })
    private void testGetDependentAttrsInActiveModel() {
        List<AttributeLookup> attributes = ratingEngineService.getDependentAttrsInActiveModel(aiRatingEngineId);
        Assert.assertNotNull(attributes);
        Assert.assertTrue(attributes.size() > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetDependentAttrsInActiveModel" })
    private void testGetDependingRatingModels() {
        List<String> attributes = new ArrayList<>();
        attributes.add("Account.LDC_Name");
        attributes.add("Account.Other");

        List<RatingModel> ratingModels = ratingEngineService.getDependingRatingModels(attributes);
        Assert.assertNotNull(ratingModels);
        Assert.assertEquals(ratingModels.size(), 1);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetDependingRatingModels" })
    private void testGetDependingRatingEngines() {
        List<String> attributes = new ArrayList<>();
        attributes.add("Account.LDC_Name");
        attributes.add("Account.Other");

        List<RatingEngine> ratingEngines = ratingEngineService.getDependingRatingEngines(attributes);
        Assert.assertNotNull(ratingEngines);
        Assert.assertEquals(ratingEngines.size(), 1);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetDependingRatingEngines" })
    private void testCreateAndUpdateModelIteration() {
        RatingEngine createdRatingEngine = ratingEngineService.getRatingEngineById(aiRatingEngineId, false, false);

        AIModel newIteration = new AIModel();
        newIteration.setCreatedBy(CREATED_BY);
        newIteration.setUpdatedBy(CREATED_BY);
        newIteration.setAdvancedModelingConfig(new CrossSellModelingConfig());
        newIteration.setRatingEngine(aiRatingEngine);
        Assert.assertThrows(LedpException.class, () -> aiModelService.createNewIteration(newIteration, aiRatingEngine));
        String derivedFromModelID = createdRatingEngine.getLatestIteration().getId();
        newIteration.setDerivedFromRatingModel(derivedFromModelID);

        Assert.assertThrows(LedpException.class, () -> aiModelService.createNewIteration(newIteration, aiRatingEngine));
        AIModel derivedModel = (AIModel) createdRatingEngine.getLatestIteration();
        derivedModel.setModelingJobStatus(JobStatus.COMPLETED);
        ratingEngineService.updateRatingModel(aiRatingEngineId, derivedModel.getId(), derivedModel);
        iteration2 = aiModelService.createNewIteration(newIteration, createdRatingEngine);

        List<RatingModel> ratingModels = ratingEngineService.getRatingModelsByRatingEngineId(aiRatingEngineId);
        Assert.assertEquals(ratingModels.size(), 2);
        Assert.assertEquals(iteration2.getIteration(), 2);

        ratingEngineService.getRatingEngineById(aiRatingEngineId, false, false);
        Assert.assertEquals(iteration2.getDerivedFromRatingModel(), derivedFromModelID);

        AIModel updatedAIModel = (AIModel) ratingEngineService.updateRatingModel(aiRatingEngineId, iteration2.getId(),
                iteration2);
        Assert.assertNotNull(updatedAIModel);
        Assert.assertEquals(iteration2.getId(), updatedAIModel.getId());
        Assert.assertEquals(iteration2.getIteration(), updatedAIModel.getIteration());
    }

    @Test(groups = "deployment", dependsOnMethods = { "testCreateAndUpdateModelIteration" })
    public void tearDelete() {
        deleteRatingEngine(aiRatingEngineId);

        List<RatingEngineSummary> ratingEngineList = getAllRatingEngineSummaries();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);
    }

    protected RatingEngine createTestRatingEngine(RatingEngineType type) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(reTestSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setUpdatedBy(CREATED_BY);
        ratingEngine.setType(type);
        // test basic creation
        ratingEngine = createRatingEngine(ratingEngine);
        return ratingEngine;
    }

    private AIModel getSpecificRatingModel() {
        RatingModel rm = getRatingModel();
        Assert.assertNotNull(rm);
        Assert.assertTrue(rm instanceof AIModel);
        return (AIModel) rm;
    }

    private List<String> generateSelectedProducts() {
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

    private void assertUpdatedAIModelWithBasicFields(AIModel aiModel) {
        Assert.assertNotNull(aiModel);
        Assert.assertEquals(1, aiModel.getIteration());

        Assert.assertNotNull(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTargetProducts());
        Assert.assertTrue(
                CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTargetProducts().contains(PRODUCT_ID1));
        Assert.assertTrue(
                CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTargetProducts().contains(PRODUCT_ID2));
        Assert.assertFalse(CollectionUtils
                .isEmpty(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTrainingProducts()));
        Assert.assertNotNull(
                CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTrainingProducts().contains(PRODUCT_ID3));
        Assert.assertEquals(aiModel.getModelingYarnJobId().toString(), APP_JOB_ID);
    }

    private void assertUpdatedModelWithRelationshipObjects(AIModel aiModel) {
        assertUpdatedAIModelWithBasicFields(aiModel);

        Assert.assertNotNull(aiModel.getTrainingSegment());
        Assert.assertNotNull(aiModel.getTrainingSegment().getName());
        Assert.assertEquals(aiModel.getTrainingSegment().getDisplayName(), TRAINING_SEGMENT_NAME);
    }

    private void assertUpdatedModelWithConfigFilters(AIModel aiModel,
            Map<CrossSellModelingConfigKeys, ModelingConfigFilter> testFilters) {
        assertUpdatedModelWithRelationshipObjects(aiModel);

        if (testFilters == null) {
            return;
        }
        Assert.assertNotNull(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getFilters());
        Assert.assertEquals(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getFilters().size(),
                testFilters.size());
        for (ModelingConfigFilter filter : testFilters.values()) {
            Assert.assertTrue(
                    CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getFilters().values().contains(filter));
        }
    }

    private void assertUpdatedModelWithModelSummary(AIModel aiModel, ModelSummary testModelSummary) {
        assertUpdatedModelWithConfigFilters(aiModel, null);

        if (testModelSummary == null) {
            return;
        }
        Assert.assertNotNull(aiModel.getModelSummaryId());
        Assert.assertEquals(aiModel.getModelSummaryId(), testModelSummary.getId());
    }

    private void assertDefaultAIModel(AIModel aiModel) {
        Assert.assertNotNull(aiModel);
        Assert.assertNotNull(aiModel.getId());
        Assert.assertEquals(aiModel.getIteration(), 1);

        Assert.assertTrue(CollectionUtils
                .isEmpty(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTargetProducts()));
        Assert.assertTrue(CollectionUtils
                .isEmpty(CrossSellModelingConfig.getAdvancedModelingConfig(aiModel).getTrainingProducts()));
        Assert.assertNull(aiModel.getTrainingSegment());
        Assert.assertNull(aiModel.getModelingYarnJobId());
    }

    protected void deleteRatingEngine(String ratingEngineId) {
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId);
        // test delete
        deleteRatingEngine(ratingEngine);
        ratingEngine = getRatingEngineById(ratingEngineId);
        Assert.assertNull(ratingEngine);
    }

    private void assertModelingQueries(EventFrontEndQuery targetQuery, EventFrontEndQuery trainingQuery,
            EventFrontEndQuery eventQuery) {
        Assert.assertNotNull(targetQuery);

        Assert.assertNotNull(trainingQuery);

        Assert.assertNotNull(eventQuery);
    }

}
