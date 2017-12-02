package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.latticeengines.domain.exposed.pls.ModelWorkflowType;
import com.latticeengines.domain.exposed.pls.ModelingConfig;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;

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

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironmentWithDummySegment();
        ratingEngine = new RatingEngine();
        ratingEngine.setDisplayName(RATING_ENGINE_NAME);
        ratingEngine.setNote(RATING_ENGINE_NOTE);
        ratingEngine.setSegment(testSegment);
        ratingEngine.setCreatedBy(CREATED_BY);
        ratingEngine.setType(RatingEngineType.AI_BASED);

        RatingEngine createdRatingEngine = ratingEngineEntityMgr.createOrUpdateRatingEngine(ratingEngine,
                mainTestTenant.getId());
        Assert.assertNotNull(createdRatingEngine);
        ratingEngineId = createdRatingEngine.getId();
        createdRatingEngine = ratingEngineEntityMgr.findById(createdRatingEngine.getId());
        Assert.assertNotNull(createdRatingEngine);
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
        aiModel.setWorkflowType(ModelWorkflowType.CROSS_SELL);
        aiModel.setTargetProducts(generateSeletedProducts());
        aiModelEntityMgr.createOrUpdate(aiModel);
        aiModelList = aiModelEntityMgr.findByRatingEngineId(ratingEngineId, null);
        Assert.assertNotNull(aiModelList);
        Assert.assertEquals(aiModelList.size(), 1);
        aiModel = aiModelEntityMgr.findById(aiModelList.get(0).getId());
        assertUpdatedAIModel(aiModel);
    }
    
    @Test(groups = "functional", dependsOnMethods= {"testBasicOperations"})
    public void testUpdateTrainingData() {
    		aiModel.setTrainingSegment(createMetadataSegment(TRAINING_SEGMENT_NAME));
    		aiModel.setTrainingProducts(generateTrainingProducts());
    		aiModel.setTargetCustomerSet("new");
    		
    		aiModel.setModelingJobId(APP_JOB_ID);
    		
    		aiModelEntityMgr.createOrUpdateAIModel(aiModel, ratingEngineId);
    		
    		aiModel = aiModelEntityMgr.findById(aiModel.getId());
    		Assert.assertNotNull(aiModel, "Could not find AIModel");
    		assertUpdatedModelWithTrainingData(aiModel);
    }
    
    @Test(groups = "functional", dependsOnMethods= {"testUpdateTrainingData"})
    public void testUpdateRefineSettings() {
        ModelingConfigFilter spendFilter = new ModelingConfigFilter(ModelingConfig.SPEND_IN_PERIOD, "Atmost", 1500);
    	 	ModelingConfigFilter quantityFilter = new ModelingConfigFilter(ModelingConfig.QUANTITY_IN_PERIOD, "Atmost", 100);
    		
	 	Map<ModelingConfig, ModelingConfigFilter> configFitlers = new HashMap<>();
	 	configFitlers.put(ModelingConfig.SPEND_IN_PERIOD, spendFilter);
	 	configFitlers.put(ModelingConfig.QUANTITY_IN_PERIOD, quantityFilter);
	 	aiModel.setModelingConfigFilters(configFitlers);
		
    		aiModelEntityMgr.createOrUpdateAIModel(aiModel, ratingEngineId);
    		
    		aiModel = aiModelEntityMgr.findById(aiModel.getId());
    		assertUpdatedModelWithConfigFilters(aiModel, configFitlers);
    }

	@Test(groups = "functional", dependsOnMethods= {"testUpdateRefineSettings"})
    public void testDelete() {
    		aiModelEntityMgr.deleteById(aiModel.getId());
    		aiModel = aiModelEntityMgr.findById(aiModel.getId());
    		Assert.assertNull(aiModel, "AIModel is not deleted");
    }
   
    private void assertUpdatedAIModel(AIModel aiModel) {
        Assert.assertNotNull(aiModel);
        Assert.assertEquals(aiModel.getId(), aiModelId);
        Assert.assertEquals(1, aiModel.getIteration());
        Assert.assertNotNull(aiModel.getRatingEngine());
        Assert.assertEquals(aiModel.getRatingEngine().getId(), ratingEngineId);

        Assert.assertNotNull(aiModel.getTargetProducts());
        Assert.assertTrue(aiModel.getTargetProducts().contains(PRODUCT_ID1));
        Assert.assertTrue(aiModel.getTargetProducts().contains(PRODUCT_ID2));
    }
    
    private void assertUpdatedModelWithTrainingData(AIModel aiModel) {
    		assertUpdatedAIModel(aiModel);
    		Assert.assertNotNull(aiModel.getTrainingSegment());
    		Assert.assertNotNull(aiModel.getTrainingSegment().getName());
    		
    		Assert.assertNotNull(aiModel.getTrainingProducts());
    		Assert.assertNotNull(aiModel.getTrainingProducts().contains(PRODUCT_ID3));
    		Assert.assertEquals(aiModel.getTargetCustomerSet(), "new");
    		
    		Assert.assertEquals(aiModel.getModelingJobId().toString(), APP_JOB_ID);
	}
    
    private void assertUpdatedModelWithConfigFilters(AIModel aiModel, Map<ModelingConfig, ModelingConfigFilter> filters) {
		assertUpdatedAIModel(aiModel);
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
        Assert.assertNotNull(aiModel.getRatingEngine());
        
        Assert.assertNull(aiModel.getTargetProducts());
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
