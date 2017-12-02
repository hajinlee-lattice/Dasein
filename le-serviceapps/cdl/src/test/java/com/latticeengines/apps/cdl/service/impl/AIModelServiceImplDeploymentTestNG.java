package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.RatingEngineService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.ModelWorkflowType;
import com.latticeengines.domain.exposed.pls.ModelingConfig;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineSummary;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.proxy.exposed.metadata.SegmentProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

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

    @BeforeClass(groups = "deployment")
    public void setup() throws KeyManagementException, NoSuchAlgorithmException, IOException, Exception {
        setupTestEnvironment();
        cdlTestDataService.populateData(mainTestTenant.getId());
        MetadataSegment createdSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(),
                constructSegment(SEGMENT_NAME));
        Assert.assertNotNull(createdSegment);
        reTestSegment = segmentProxy.getMetadataSegmentByName(mainTestTenant.getId(),
                createdSegment.getName());
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
        
        Assert.assertNotNull(aiRatingEngine.getRatingModels());
        Assert.assertTrue(MapUtils.isEmpty(aiRatingEngine.getCountsAsMap()));
        Assert.assertEquals(aiRatingEngine.getRatingModels().size(), 1);
        
        Iterator<RatingModel> it = aiRatingEngine.getRatingModels().iterator();
        RatingModel rm = it.next();
        aiRatingModelId = rm.getId();
        
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

    
    @Test(groups = "deployment", dependsOnMethods= {"testCreate"})
    private void testFindAndUpdateRatingModelBasicFields() {
        // test get specific rating model
        RatingModel rm = getRatingModel();
        Assert.assertNotNull(rm);
        Assert.assertTrue(rm instanceof AIModel);
        
        // test update rating model
        AIModel aiModel = (AIModel)rm;
        assertDefaultAIModel(aiModel);
        
        aiModel.setWorkflowType(ModelWorkflowType.CROSS_SELL);
        aiModel.setTargetProducts(generateSeletedProducts());
		aiModel.setTrainingProducts(generateTrainingProducts());
		aiModel.setTargetCustomerSet("new");
		aiModel.setModelingJobId(APP_JOB_ID);

        updateRatingModel(aiModel);
        rm = (AIModel) getRatingModel();
        Assert.assertTrue(rm instanceof AIModel);
        assertUpdatedAIModelWithBasicFields((AIModel) rm);
    }
    
    @Test(groups = "deployment", dependsOnMethods= {"testFindAndUpdateRatingModelBasicFields"})
    private void testUpdateRatingModelRelationshipObjects() {
        // test get specific rating model
        RatingModel rm = getRatingModel();
        Assert.assertNotNull(rm);
        Assert.assertTrue(rm instanceof AIModel);
        AIModel aiModel = (AIModel)rm;
        
        MetadataSegment trainingSegment = segmentProxy.createOrUpdateSegment(mainTestTenant.getId(),
                constructSegment(TRAINING_SEGMENT_NAME));
        Assert.assertNotNull(trainingSegment);
        aiModel.setTrainingSegment(trainingSegment);
        
        updateRatingModel(aiModel);
        rm = (AIModel) getRatingModel();
        Assert.assertTrue(rm instanceof AIModel);
        assertUpdatedModelWithRelationshipObjects((AIModel) rm);
    }

    @Test(groups = "deployment", dependsOnMethods= {"testUpdateRatingModelRelationshipObjects"})
    private void testUpdateRatingModelConfigFitlers() {
        // test get specific rating model
        RatingModel rm = getRatingModel();
        Assert.assertNotNull(rm);
        Assert.assertTrue(rm instanceof AIModel);
        AIModel aiModel = (AIModel)rm;
        
        ModelingConfigFilter spendFilter = new ModelingConfigFilter(ModelingConfig.SPEND_IN_PERIOD, "Atmost", 1500);
	 	ModelingConfigFilter quantityFilter = new ModelingConfigFilter(ModelingConfig.QUANTITY_IN_PERIOD, "Atmost", 100);
		
	 	Map<ModelingConfig, ModelingConfigFilter> configFitlers = new HashMap<>();
	 	configFitlers.put(ModelingConfig.SPEND_IN_PERIOD, spendFilter);
	 	configFitlers.put(ModelingConfig.QUANTITY_IN_PERIOD, quantityFilter);
	 	aiModel.setModelingConfigFilters(configFitlers);
	 	
        updateRatingModel(aiModel);
        rm = (AIModel) getRatingModel();
        Assert.assertTrue(rm instanceof AIModel);
        
        assertUpdatedModelWithConfigFilters((AIModel) rm, configFitlers);
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
    
    private void assertUpdatedModelWithConfigFilters(AIModel aiModel, Map<ModelingConfig, ModelingConfigFilter> filters) {
    	    assertUpdatedModelWithRelationshipObjects(aiModel);
    	    
		Assert.assertNotNull(aiModel.getModelingConfigFilters());
		Assert.assertEquals(aiModel.getModelingConfigFilters().size(), filters.size());
		for (ModelingConfigFilter filter: filters.values()) {
			Assert.assertTrue(aiModel.getModelingConfigFilters().values().contains(filter));
		}
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
    
    @Test(groups = "deployment", dependsOnMethods= {"testUpdateRatingModelConfigFitlers"})
    public void tearDelete() {
        deleteRatingEngine(aiRatingEngineId);
        
        List<RatingEngineSummary> ratingEngineList = getAllRatingEngineSummaries();
        Assert.assertNotNull(ratingEngineList);
        Assert.assertEquals(ratingEngineList.size(), 0);
    }

	protected void deleteRatingEngine(String ratingEngineId) {
		RatingEngine ratingEngine = getRatingEngineById(ratingEngineId);
        // test delete
        deleteRatingEngine(ratingEngine);
        ratingEngine = getRatingEngineById(ratingEngineId);
        Assert.assertNull(ratingEngine);
	}

}
