package com.latticeengines.dataplatform.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.util.Arrays;
import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.domain.Classifier;
import com.latticeengines.dataplatform.exposed.domain.Field;
import com.latticeengines.dataplatform.exposed.domain.Job;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;

public class JobEntityMgrImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private JobEntityMgrImpl jobEntityMgr;

    private Job job;

    @Override
    protected boolean doYarnClusterSetup() {
        return false;
    }

    @BeforeClass(groups = "functional")
    public void setup() {
        jobEntityMgr.deleteStoreFile();

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

        String metadata = classifier.toString();

        job = new Job();
        job.setId("application_12345_00001");
        Properties appMasterProperties = new Properties();
        appMasterProperties.setProperty("QUEUE", "Priority0.0");
        Properties containerProperties = new Properties();
        containerProperties.setProperty("METADATA", metadata);

        job.setAppMasterProperties(appMasterProperties);
        job.setContainerProperties(containerProperties);
        
        Model model = new Model();
        model.setId("model_xyz");
        model.setCustomer("INTERNAL");
        model.setTable("iris");
        
        job.setModel(model);
    }

    private void verifyRetrievedJob(String id) {
        Job retrievedJob = jobEntityMgr.getById(id);
        assertNotNull(retrievedJob);
        assertEquals(id, retrievedJob.getId());
        assertEquals(job.getAppMasterProperties().getProperty("QUEUE"), retrievedJob.getAppMasterProperties()
                .getProperty("QUEUE"));
        assertEquals(job.getContainerProperties().getProperty("METADATA"), retrievedJob.getContainerProperties()
                .getProperty("METADATA"));
    }

    @Test(groups = "functional")
    public void postThenSave() {
        jobEntityMgr.post(job);
        verifyRetrievedJob(job.getId());
        jobEntityMgr.save();
    }

    @Test(groups = "functional", dependsOnMethods = { "postThenSave" })
    public void clear() {
        jobEntityMgr.clear();
        assertNull(jobEntityMgr.getById(job.getId()));
    }

    @Test(groups = "functional", dependsOnMethods = { "clear" })
    public void load() {
        jobEntityMgr.load();
        verifyRetrievedJob(job.getId());
    }
}
