package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelWorkflowType;
import com.latticeengines.domain.exposed.pls.ModelingConfig;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
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
    private CDLTestDataService cdlTestDataService;

    protected MetadataSegment reTestSegment;

    protected RatingEngine aiRatingEngine;
    protected String aiRatingEngineId;
    protected String aiRatingModelId;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws KeyManagementException, NoSuchAlgorithmException, IOException, Exception {
        setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId());
        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(),
                constructSegment(SEGMENT_NAME));
        Assert.assertNotNull(createdSegment);
        reTestSegment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(), createdSegment.getName());
        log.info(String.format("Created metadata segment with name %s", reTestSegment.getName()));

    }

    protected RatingEngine createRatingEngine(RatingEngine ratingEngine) {
        return ratingEngineService.createOrUpdate(ratingEngine, mainTestTenant.getId());
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

    @Test(groups = "deployment")
    public void testCreate() {
        aiRatingEngine = createRatingEngine(RatingEngineType.AI_BASED);
        Assert.assertEquals(aiRatingEngine.getType(), RatingEngineType.AI_BASED);
        aiRatingEngineId = aiRatingEngine.getId();

        List<RatingModel> ratingModels = ratingEngineService.getRatingModelsByRatingEngineId(aiRatingEngineId);
        Assert.assertEquals(ratingModels.size(), 1);
        aiRatingModelId = ratingModels.get(0).getId();
        Assert.assertNotNull(aiRatingModelId, "AIRatingModel is null");
    }

    protected RatingEngine createRatingEngine(RatingEngineType type) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(reTestSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(type);
        // test basic creation
        ratingEngine = createRatingEngine(ratingEngine);
        return ratingEngine;
    }

    @Test(groups = "deployment", dependsOnMethods = { "testCreate" })
    private void testFindAndUpdateRatingModelBasicFields() {
        // test update rating model
        AIModel aiModel = getSpecificRatingModel();
        assertDefaultAIModel(aiModel);

        aiModel.setWorkflowType(ModelWorkflowType.CROSS_SELL);
        aiModel.setTargetProducts(generateSeletedProducts());
        aiModel.setTrainingProducts(generateTrainingProducts());
        aiModel.setTargetCustomerSet("new");
        aiModel.setModelingJobId(APP_JOB_ID);

        updateRatingModel(aiModel);
        assertUpdatedAIModelWithBasicFields(getSpecificRatingModel());
    }

    private AIModel getSpecificRatingModel() {
        RatingModel rm = getRatingModel();
        Assert.assertNotNull(rm);
        Assert.assertTrue(rm instanceof AIModel);
        AIModel aiModel = (AIModel) rm;
        return aiModel;
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
    private void testUpdateRatingModelConfigFitlers() {
        // test get specific rating model
        AIModel aiModel = getSpecificRatingModel();

        ModelingConfigFilter spendFilter = new ModelingConfigFilter(ModelingConfig.SPEND_IN_PERIOD, "Atmost", 1500);
        ModelingConfigFilter quantityFilter = new ModelingConfigFilter(ModelingConfig.QUANTITY_IN_PERIOD, "Atmost",
                100);

        Map<ModelingConfig, ModelingConfigFilter> configFitlers = new HashMap<>();
        configFitlers.put(ModelingConfig.SPEND_IN_PERIOD, spendFilter);
        configFitlers.put(ModelingConfig.QUANTITY_IN_PERIOD, quantityFilter);
        aiModel.setModelingConfigFilters(configFitlers);

        updateRatingModel(aiModel);
        assertUpdatedModelWithConfigFilters(getSpecificRatingModel(), configFitlers);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testUpdateRatingModelConfigFitlers" })
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
            modelSummary = ModelSummaryUtils.createModelSummary(internalResourceProxy, mainTestTenant,
                    modelConfiguration);
        } catch (IOException e) {
            Assert.fail("Could not create ModelSummary", e);
        }
        Assert.assertNotNull(modelSummary);
        log.info("Created ModelSummary ID: " + modelSummary.getId());

        ModelSummary retModelSummary = internalResourceProxy.getModelSummaryFromModelId(modelConfiguration.getModelId(),
                CustomerSpace.parse(mainTestTenant.getId()));
        Assert.assertNotNull(retModelSummary);
        Assert.assertNotNull(retModelSummary.getId());

        AIModel aiModel = getSpecificRatingModel();

        ModelSummary selectedModelSummary = new ModelSummary();
        selectedModelSummary.setId(retModelSummary.getId());
        aiModel.setModelSummary(selectedModelSummary);

        updateRatingModel(aiModel);
        assertUpdatedModelWithModelSummary(getSpecificRatingModel(), retModelSummary);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testUpdateRatingModelWithModelSummary" })
    public void tearDelete() {
        deleteRatingEngine(aiRatingEngineId);

        List<RatingEngineSummary> ratingEngineList = getAllRatingEngineSummaries();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);
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

    private void assertUpdatedAIModelWithBasicFields(AIModel aiModel) {
        Assert.assertNotNull(aiModel);
        Assert.assertEquals(1, aiModel.getIteration());

        Assert.assertNotNull(aiModel.getTargetProducts());
        Assert.assertTrue(aiModel.getTargetProducts().contains(PRODUCT_ID1));
        Assert.assertTrue(aiModel.getTargetProducts().contains(PRODUCT_ID2));
        Assert.assertEquals(aiModel.getWorkflowType(), ModelWorkflowType.CROSS_SELL);
        Assert.assertNotNull(aiModel.getTrainingProducts());
        Assert.assertNotNull(aiModel.getTrainingProducts().contains(PRODUCT_ID3));
        Assert.assertEquals(aiModel.getTargetCustomerSet(), "new");
        Assert.assertEquals(aiModel.getModelingJobId().toString(), APP_JOB_ID);
    }

    private void assertUpdatedModelWithRelationshipObjects(AIModel aiModel) {
        assertUpdatedAIModelWithBasicFields(aiModel);

        Assert.assertNotNull(aiModel.getTrainingSegment());
        Assert.assertNotNull(aiModel.getTrainingSegment().getName());
        Assert.assertEquals(aiModel.getTrainingSegment().getDisplayName(), TRAINING_SEGMENT_NAME);
    }

    private void assertUpdatedModelWithConfigFilters(AIModel aiModel,
            Map<ModelingConfig, ModelingConfigFilter> testFilters) {
        assertUpdatedModelWithRelationshipObjects(aiModel);

        if (testFilters == null) {
            return;
        }
        Assert.assertNotNull(aiModel.getModelingConfigFilters());
        Assert.assertEquals(aiModel.getModelingConfigFilters().size(), testFilters.size());
        for (ModelingConfigFilter filter : testFilters.values()) {
            Assert.assertTrue(aiModel.getModelingConfigFilters().values().contains(filter));
        }
    }

    private void assertUpdatedModelWithModelSummary(AIModel aiModel, ModelSummary testModelSummary) {
        assertUpdatedModelWithConfigFilters(aiModel, null);

        if (testModelSummary == null) {
            return;
        }
        Assert.assertNotNull(aiModel.getModelSummary());
        Assert.assertEquals(aiModel.getModelSummary().getId(), testModelSummary.getId());
    }

    private void assertDefaultAIModel(AIModel aiModel) {
        Assert.assertNotNull(aiModel);
        Assert.assertNotNull(aiModel.getId());
        Assert.assertEquals(aiModel.getIteration(), 1);

        Assert.assertNull(aiModel.getTargetProducts());
        Assert.assertNull(aiModel.getTrainingProducts());
        Assert.assertNull(aiModel.getTrainingSegment());
        Assert.assertNull(aiModel.getWorkflowType());
        Assert.assertNull(aiModel.getTargetProducts());
        Assert.assertNull(aiModel.getTrainingProducts());
        Assert.assertNull(aiModel.getTargetCustomerSet());
        Assert.assertNull(aiModel.getModelingJobId());
    }

    protected void deleteRatingEngine(String ratingEngineId) {
        RatingEngine ratingEngine = getRatingEngineById(ratingEngineId);
        // test delete
        deleteRatingEngine(ratingEngine);
        ratingEngine = getRatingEngineById(ratingEngineId);
        Assert.assertNull(ratingEngine);
    }

}
