package com.latticeengines.dataplatform.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.modeling.ModelDefinitionEntityMgr;
import com.latticeengines.dataplatform.entitymanager.modeling.ModelEntityMgr;
import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.Field;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class JobEntityMgrImplTestNG extends DataPlatformFunctionalTestNGBase {

    private ModelingJob modelingJob;
    private ModelDefinition modelDef = new ModelDefinition();
    Model model = new Model();

    @Autowired
    protected JobEntityMgr jobEntityMgr;

    @Autowired
    protected ModelEntityMgr modelEntityMgr;

    @Autowired
    protected ModelDefinitionEntityMgr modelDefinitionEntityMgr;

    @BeforeClass(groups = {"functional", "functional.production"})
    public void setup() {

        Classifier classifier = new Classifier();
        classifier.setName("NeuralNetworkClassifier");
        classifier.setSchemaHdfsPath("/datascientist1/iris.json");
        Field sepalLength = new Field();
        sepalLength.setName("sepal_length");
        sepalLength.setType(Arrays.<String> asList(new String[] { "float", "0.0" }));
        Field sepalWidth = new Field();
        sepalWidth.setName("sepal_width");
        sepalWidth.setType(Arrays.<String> asList(new String[] { "float", "0.0" }));
        Field petalLength = new Field();
        petalLength.setName("petal_length");
        petalLength.setType(Arrays.<String> asList(new String[] { "float", "0.0" }));
        Field petalWidth = new Field();
        petalWidth.setName("petal_width");
        petalWidth.setType(Arrays.<String> asList(new String[] { "float", "0.0" }));
        Field category = new Field();
        category.setName("category");
        category.setType(Arrays.<String> asList(new String[] { "string", "null" }));

        classifier.addFeature(sepalLength.getName());
        classifier.addFeature(sepalWidth.getName());
        classifier.addFeature(petalLength.getName());
        classifier.addTarget(category.getName());

        classifier.setTrainingDataHdfsPath("/training/nn_train.dat");
        classifier.setTestDataHdfsPath("/test/nn_test.dat");
        classifier.setPythonScriptHdfsPath("/datascientist1/nn_train.py");
        classifier.setModelHdfsDir("/datascientist1/result");

        //
        // job
        modelingJob = new ModelingJob();
        modelingJob.setId("application_12345_00001_" + suffix);
        modelingJob.setClient("CLIENT");
        modelingJob.setCustomer("SomeCustomer");
        Properties appMasterProperties = new Properties();
        appMasterProperties.setProperty("QUEUE", LedpQueueAssigner.getModelingQueueNameForSubmission());
        Properties containerProperties = new Properties();
        // setting container metadata
        String metadata = classifier.toString();
        containerProperties.setProperty("METADATA", metadata);

        modelingJob.setAppMasterPropertiesObject(appMasterProperties);
        modelingJob.setContainerPropertiesObject(containerProperties);

        //
        // model
        LogisticRegressionAlgorithm logisticRegressionAlgorithm = new LogisticRegressionAlgorithm();
        logisticRegressionAlgorithm.setPriority(0);
        logisticRegressionAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=0");
        logisticRegressionAlgorithm.setSampleName("s0");

        DecisionTreeAlgorithm decisionTreeAlgorithm = new DecisionTreeAlgorithm();
        decisionTreeAlgorithm.setPriority(1);
        decisionTreeAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=1");
        decisionTreeAlgorithm.setSampleName("s1");

        modelDef.setName("Model Definition_" + suffix);
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { decisionTreeAlgorithm,
                logisticRegressionAlgorithm }));
        //
        // in the application, it is assumed that the model definition is
        // defined in the metadata db
        // also, modelDef 'name' should be unique
        modelDefinitionEntityMgr.createOrUpdate(modelDef);
        //
        // setup and persist a test model first
        model.setId("model_" + suffix);
        model.setName("MODEL_NAME_" + suffix);
        model.setModelDefinition(modelDef);
        modelEntityMgr.create(model);
        // link model--job
        modelingJob.setModel(model);
    }

    @AfterClass(groups = {"functional", "functional.production"})
    public void tearDown(){
        modelEntityMgr.delete(model);
        modelDefinitionEntityMgr.delete(modelDef);
    }

    private void assertJobsEqual(ModelingJob originalJob, ModelingJob retrievedJob) {
        assertEquals(originalJob.getId(), retrievedJob.getId());
        assertEquals(originalJob.getAppId(), retrievedJob.getAppId());
        assertEquals(originalJob.getAppMasterPropertiesObject(), retrievedJob.getAppMasterPropertiesObject());
        assertEquals(originalJob.getChildIds(), retrievedJob.getChildIds());
        assertEquals(originalJob.getClient(), retrievedJob.getClient());
        assertEquals(originalJob.getContainerPropertiesObject(), retrievedJob.getContainerPropertiesObject());
        assertEquals(originalJob.getModel().getPid(), retrievedJob.getModel().getPid());
        assertEquals(originalJob.getParentPid(), retrievedJob.getParentPid());

        assertEquals(modelingJob.getAppMasterPropertiesObject().getProperty("QUEUE"), retrievedJob
                .getAppMasterPropertiesObject().getProperty("QUEUE"));
        assertEquals(modelingJob.getContainerPropertiesObject().getProperty("METADATA"), retrievedJob
                .getContainerPropertiesObject().getProperty("METADATA"));

    }

    @Test(groups = {"functional", "functional.production"})
    public void testPersist() {
        jobEntityMgr.create(modelingJob);
    }

    @Test(groups = {"functional", "functional.production"}, dependsOnMethods = { "testPersist" })
    public void testRetrieval() {
        ModelingJob retrievedJob = new ModelingJob();
        retrievedJob.setPid(modelingJob.getPid());
        retrievedJob = (ModelingJob)jobEntityMgr.findByKey((Job)retrievedJob); // /
                                                             // getByKey(retrievedJob);
        // assert for correctness
        assertJobsEqual(modelingJob, retrievedJob);
    }

    @Test(groups = {"functional", "functional.production"}, dependsOnMethods = { "testPersist" })
    public void testUpdate() {
        assertNotNull(modelingJob.getPid());
        Properties appMasterProp = modelingJob.getAppMasterPropertiesObject();
        appMasterProp.setProperty("QUEUE", "Priority1.0");
        modelingJob.setAppMasterPropertiesObject(appMasterProp);
        modelingJob.setClient("NEW CLIENT");
        jobEntityMgr.update(modelingJob);

        testRetrieval();
    }

    @Test(groups = {"functional", "functional.production"}, dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        jobEntityMgr.delete(modelingJob);
    }

}
