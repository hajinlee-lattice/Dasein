package com.latticeengines.dataplatform.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.modeling.ModelDefinitionEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.ModelEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.LogisticRegressionAlgorithm;

public class ModelEntityMgrImplTestNG extends DataPlatformFunctionalTestNGBase {

    private Model model;
    private ModelDefinition modelDef = new ModelDefinition();

    @Autowired
    protected ModelEntityMgr modelEntityMgr;

    @Autowired
    protected ModelDefinitionEntityMgr modelDefinitionEntityMgr;

    @BeforeClass(groups = { "functional", "functional.production" })
    public void setup() {
        ModelingJob job1 = new ModelingJob();
        job1.setId("application_12345_00001_" + suffix);
        job1.setClient("CLIENT 1");
        ModelingJob job2 = new ModelingJob();
        job2.setId("application_12345_00002_" + suffix);
        job2.setClient("CLIENT 2");

        LogisticRegressionAlgorithm logisticRegressionAlgorithm = new LogisticRegressionAlgorithm();
        logisticRegressionAlgorithm.setPriority(0);
        logisticRegressionAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=0");
        logisticRegressionAlgorithm.setSampleName("s0");

        DecisionTreeAlgorithm decisionTreeAlgorithm = new DecisionTreeAlgorithm();
        decisionTreeAlgorithm.setPriority(1);
        decisionTreeAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=1");
        decisionTreeAlgorithm.setSampleName("s1");

        modelDef.setName("Model Definition For Demo_" + suffix);
        modelDef.addAlgorithm(logisticRegressionAlgorithm);
        modelDef.addAlgorithm(decisionTreeAlgorithm);
        //
        // in the application, it is assumed that the model definition is
        // defined in the metadata db
        // also, modelDef 'name' should be unique
        modelDefinitionEntityMgr.createOrUpdate(modelDef);

        model = new Model();
        model.addModelingJob(job1);
        model.addModelingJob(job2);

        model.setId("model_12345_0001_" + suffix);
        model.setName("MODEL TEST NAME_" + suffix);
        model.setModelDefinition(modelDef);
    }

    @AfterClass(groups = { "functional", "functional.production" })
    public void tearDown() {
        if (modelDef.getModels() != null) {
            modelDef.getModels().remove(model);
        }
        modelDefinitionEntityMgr.delete(modelDef);
    }

    private void assertModelsEqual(Model originalModel, Model retrievedModel) {
        assertNotNull(retrievedModel);
        assertEquals(originalModel.getId(), retrievedModel.getId());
        assertEquals(originalModel.getCustomer(), retrievedModel.getCustomer());
        assertEquals(originalModel.getDataFormat(), retrievedModel.getDataFormat());
        assertEquals(originalModel.getDataHdfsPath(), retrievedModel.getDataHdfsPath());
        assertEquals(originalModel.getFeatures(), retrievedModel.getFeatures());
        assertEquals(originalModel.getKeyCols(), retrievedModel.getKeyCols());
        assertEquals(originalModel.getMetadataHdfsPath(), retrievedModel.getMetadataHdfsPath());
        assertEquals(originalModel.getMetadataTable(), retrievedModel.getMetadataTable());
        assertEquals(originalModel.getModelHdfsDir(), retrievedModel.getModelHdfsDir());
        assertEquals(originalModel.getName(), retrievedModel.getName());
        assertEquals(originalModel.getSampleHdfsPath(), retrievedModel.getSampleHdfsPath());
        assertEquals(originalModel.getSchemaHdfsPath(), retrievedModel.getSchemaHdfsPath());
        assertEquals(originalModel.getTable(), retrievedModel.getTable());
        assertEquals(originalModel.getTargets(), retrievedModel.getTargets());

    }

    @Test(groups = { "functional", "functional.production" })
    public void testPersist() {
        modelEntityMgr.create(model);
    }

    @Test(groups = { "functional", "functional.production" }, dependsOnMethods = { "testPersist" })
    public void testRetrieval() {
        Model retrievedModel = new Model();
        retrievedModel.setPid(model.getPid());
        retrievedModel = modelEntityMgr.findByKey(retrievedModel);
        // assert for correctness
        assertModelsEqual(model, retrievedModel);
    }

    @Test(groups = { "functional", "functional.production" }, dependsOnMethods = { "testPersist" })
    public void testUpdate() {
        assertNotNull(model.getPid());
        model.setCustomer("NEW CUSTOMER");
        model.setName("NEW NAME");

        modelEntityMgr.update(model);
        testRetrieval();
    }

    @Test(groups = { "functional", "functional.production" }, dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        modelEntityMgr.delete(model);
    }

}
