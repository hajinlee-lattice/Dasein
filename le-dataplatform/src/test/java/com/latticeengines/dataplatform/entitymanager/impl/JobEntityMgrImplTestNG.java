package com.latticeengines.dataplatform.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.Properties;

import org.springframework.transaction.annotation.Transactional;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataplatform.Algorithm;
import com.latticeengines.domain.exposed.dataplatform.Classifier;
import com.latticeengines.domain.exposed.dataplatform.Field;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;
import com.latticeengines.domain.exposed.dataplatform.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.dataplatform.algorithm.LogisticRegressionAlgorithm;

public class JobEntityMgrImplTestNG extends DataPlatformFunctionalTestNGBase {
 
    private Job job;

    @Override
    protected boolean doYarnClusterSetup() {
        return false;
    }

    @BeforeClass(groups = "functional")
    public void setup() {
        // / jobEntityMgr.deleteStoreFile();
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
        job = new Job();
        job.setId("application_12345_00001");
        job.setClient("CLIENT");
        Properties appMasterProperties = new Properties();
        appMasterProperties.setProperty("QUEUE", "Priority0.0");
        Properties containerProperties = new Properties();
        // setting container metadata
        String metadata = classifier.toString();
        containerProperties.setProperty("METADATA", metadata);

        job.setAppMasterPropertiesObject(appMasterProperties);
        job.setContainerPropertiesObject(containerProperties);

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
        
        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model Definition For Demo");
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { decisionTreeAlgorithm,
                logisticRegressionAlgorithm }));
        // 
        // in the application, it is assumed that the model definition is defined in the metadata db
        // also, modelDef 'name' should be unique
        modelDefinitionEntityMgr.createOrUpdate(modelDef);
        // 
        // setup and persist a test model first
        Model model = new Model();                
        model.setId("model_"+this.getClass().getSimpleName()+"_0001");
        model.setName("MODEL TEST NAME");
        model.setModelDefinition(modelDef);
        modelEntityMgr.create(model);         
        // link model--job
        job.setModel(model);
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        
    }
    
    private void assertJobsEqual(Job originalJob,         Job retrievedJob) {
        assertEquals(retrievedJob.getId(), retrievedJob.getId());
        assertEquals(retrievedJob.getAppId(), retrievedJob.getAppId());
        assertEquals(retrievedJob.getAppMasterPropertiesObject(), retrievedJob.getAppMasterPropertiesObject());
        assertEquals(retrievedJob.getChildJobIds(), retrievedJob.getChildJobIds());
        assertEquals(retrievedJob.getClient(), retrievedJob.getClient());
        assertEquals(retrievedJob.getContainerPropertiesObject(), retrievedJob.getContainerPropertiesObject());
        assertEquals(retrievedJob.getModel(), retrievedJob.getModel());
        assertEquals(retrievedJob.getParentJobId(), retrievedJob.getParentJobId());

        /*
         * assertEquals(job.getAppMasterPropertiesObject().getProperty("QUEUE"),
         * retrievedJob.getAppMasterPropertiesObject() .getProperty("QUEUE"));
         * assertEquals
         * (job.getContainerPropertiesObject().getProperty("METADATA"),
         * retrievedJob.getContainerPropertiesObject()
         * .getProperty("METADATA"));
         */
    }

    @Transactional
    @Test(groups = "functional")
    public void testPersist() {
        jobEntityMgr.create(job);
    }

    @Transactional
    @Test(groups = "functional", dependsOnMethods = { "testPersist" })
    public void testRetrieval() {
        Job retrievedJob = new Job();        
        retrievedJob.setPid(job.getPid());
        retrievedJob = jobEntityMgr.findByKey(retrievedJob); /// getByKey(retrievedJob);
        // assert for correctness
        assertJobsEqual(job, retrievedJob);
    }

    @Transactional
    @Test(groups = "functional", dependsOnMethods = { "testPersist" })
    public void testUpdate() {
        assertNotNull(job.getPid());
        Properties appMasterProp = job.getAppMasterPropertiesObject();
        appMasterProp.setProperty("QUEUE", "Priority1.0");
        job.setAppMasterPropertiesObject(appMasterProp);
        job.setClient("NEW CLIENT");
        jobEntityMgr.update(job);

        testRetrieval();
    }

    @Transactional
    @Test(groups = "functional", dependsOnMethods = { "testUpdate" })
    public void testDelete() {
        jobEntityMgr.delete(job);
    }

    /*
     * @Test(groups = "functional") public void postThenSave() {
     * jobEntityMgr.post(job); verifyRetrievedJob(job.getId());
     * jobEntityMgr.save(); }
     */

    /*
     * @Test(groups = "functional", dependsOnMethods = { "postThenSave" })
     * public void clear() { jobEntityMgr.clear();
     * assertNull(jobEntityMgr.getById(job.getId())); }
     * 
     * @Test(groups = "functional", dependsOnMethods = { "clear" }) public void
     * load() { jobEntityMgr.load(); verifyRetrievedJob(job.getId()); }
     */
}
