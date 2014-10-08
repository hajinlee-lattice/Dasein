package com.latticeengines.dataplatform.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.latticeengines.dataplatform.entitymanager.modeling.ModelDefinitionEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.ModelEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataplatform.Job;
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

    @Override
    protected boolean doYarnClusterSetup() {
        return false;
    }

    @BeforeClass(groups = "functional")
    public void setup() {
        ModelingJob job1 = new ModelingJob();
        job1.setId("application_12345_00001");
        job1.setClient("CLIENT 1");
        ModelingJob job2 = new ModelingJob();
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

        modelDef.setName("Model Definition For Demo");
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
        assertEquals(model.getKeyCols(), retrievedModel.getKeyCols());
        assertEquals(model.getMetadataHdfsPath(), retrievedModel.getMetadataHdfsPath());
        assertEquals(model.getMetadataTable(), retrievedModel.getMetadataTable());
        assertEquals(model.getModelHdfsDir(), retrievedModel.getModelHdfsDir());
        assertEquals(model.getName(), retrievedModel.getName());
        assertEquals(model.getSampleHdfsPath(), retrievedModel.getSampleHdfsPath());
        assertEquals(model.getSchemaHdfsPath(), retrievedModel.getSchemaHdfsPath());
        assertEquals(model.getTable(), retrievedModel.getTable());
        assertEquals(model.getTargets(), retrievedModel.getTargets());

    }

    @Test(groups = "functional")
    public void testPersist() {
        modelEntityMgr.create(model);
    }

    @Test(groups = "functional", dependsOnMethods = { "testPersist" })
    public void testRetrieval() {
        Model retrievedModel = new Model();
        retrievedModel.setPid(model.getPid());
        retrievedModel = modelEntityMgr.findByKey(retrievedModel);
        // assert for correctness
        assertModelsEqual(model, retrievedModel);
    }

    @Test(groups = "functional", dependsOnMethods = { "testPersist" })
    public void testUpdate() {
        assertNotNull(model.getPid());
        model.setCustomer("NEW CUSTOMER");
        model.setName("NEW NAME");

        modelEntityMgr.update(model);
        testRetrieval();
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        modelEntityMgr.delete(model);
    }

}
