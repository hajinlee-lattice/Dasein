package com.latticeengines.dataplatform.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;

import org.springframework.transaction.annotation.Transactional;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataplatform.Algorithm;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;
import com.latticeengines.domain.exposed.dataplatform.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.dataplatform.algorithm.LogisticRegressionAlgorithm;

public class ModelEntityMgrImplTestNG extends DataPlatformFunctionalTestNGBase {

    private Model model;

    @Override
    protected boolean doYarnClusterSetup() {
        return false;
    }

    @BeforeClass(groups = "functional")
    public void setup() {
        /// modelEntityMgr.deleteStoreFile();
        Job job1 = new Job();
        job1.setId("application_12345_00001");
        job1.setClient("CLIENT 1"); 
        Job job2 = new Job();
        job2.setId("application_12345_00002");
        job2.setClient("CLIENT 2");

        LogisticRegressionAlgorithm logisticRegressionAlgorithm = new LogisticRegressionAlgorithm();
        logisticRegressionAlgorithm.setPriority(0);
        logisticRegressionAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=0");
        logisticRegressionAlgorithm.setSampleName("s0");

        DecisionTreeAlgorithm decisionTreeAlgorithm = new DecisionTreeAlgorithm();
        decisionTreeAlgorithm.setPriority(1);
        decisionTreeAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=1");
        decisionTreeAlgorithm.setSampleName("s1");
        
        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model Definition For Demo");
        modelDef.setAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { decisionTreeAlgorithm,
                logisticRegressionAlgorithm }));
        
        // 
        // in the application, it is assumed that the model definition is defined in the metadata db
        // also, modelDef 'name' should be unique
        modelDefinitionEntityMgr.createOrUpdate(modelDef);

        model = new Model();        
        model.addJob(job1);
        model.addJob(job2);
        
        model.setId("model_12345_0001");
        model.setName("MODEL TEST NAME");
        model.setModelDefinition(modelDef);        
    }

    private void assertModelsEqual(Model originalModel, Model retrievedModel) {
        assertNotNull(retrievedModel);
        assertEquals(model.getId(), retrievedModel.getId());
        assertEquals(model.getCustomer(), retrievedModel.getCustomer());
        assertEquals(model.getDataFormat(), retrievedModel.getDataFormat());
        assertEquals(model.getDataHdfsPath(), retrievedModel.getDataHdfsPath());
        assertEquals(model.getFeatures(), retrievedModel.getFeatures());
        assertEquals(model.getJobs(), retrievedModel.getJobs());
        assertEquals(model.getKeyCols(), retrievedModel.getKeyCols());
        assertEquals(model.getMetadataHdfsPath(), retrievedModel.getMetadataHdfsPath());
        assertEquals(model.getMetadataTable(), retrievedModel.getMetadataTable());
        // assertEquals(model.getModelDefinition(), retr
        assertEquals(model.getModelHdfsDir(), retrievedModel.getModelHdfsDir());
        assertEquals(model.getName(), retrievedModel.getName());
        assertEquals(model.getSampleHdfsPath(), retrievedModel.getSampleHdfsPath());
        assertEquals(model.getSchemaHdfsPath(), retrievedModel.getSchemaHdfsPath());
        assertEquals(model.getTable(), retrievedModel.getTable());
        assertEquals(model.getTargets(), retrievedModel.getTargets());

    }

    @Transactional
    @Test(groups = "functional")
    public void testPersist() {
        modelEntityMgr.create(model);
    }

    @Transactional
    @Test(groups = "functional", dependsOnMethods = { "testPersist" })
    public void testRetrieval() {
        Model retrievedModel = new Model();
        retrievedModel.setPid(model.getPid());
        retrievedModel = modelEntityMgr.findByKey(retrievedModel);  ///getByKey(retrievedModel);
        // assert for correctness
        assertModelsEqual(model, retrievedModel);
        assertEquals(2, model.getJobs().size());
    }

    @Transactional
    @Test(groups = "functional", dependsOnMethods = { "testPersist" })
    public void testUpdate() {
        assertNotNull(model.getPid());
        model.setCustomer("NEW CUSTOMER");
        model.setName("NEW NAME");
        
        modelEntityMgr.update(model);        
        testRetrieval();
    }

    @Transactional
    @Test(groups = "functional", dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        modelEntityMgr.delete(model);
    }

    /*
     * @Test(groups = "functional") public void postThenSave() {
     * modelEntityMgr.post(model); verifyRetrievedModel(model.getId());
     * modelEntityMgr.save(); }
     * 
     * @Test(groups = "functional", dependsOnMethods = { "postThenSave" })
     * public void clear() { modelEntityMgr.clear();
     * assertNull(modelEntityMgr.getById(model.getId())); }
     * 
     * @Test(groups = "functional", dependsOnMethods = { "clear" }) public void
     * load() { modelEntityMgr.load(); verifyRetrievedModel(model.getId()); }
     */
}
