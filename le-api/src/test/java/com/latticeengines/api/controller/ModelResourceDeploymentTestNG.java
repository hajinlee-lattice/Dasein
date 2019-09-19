package com.latticeengines.api.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.modeling.ThrottleConfigurationEntityMgr;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

public class ModelResourceDeploymentTestNG extends BaseModelResourceDeploymentTestNG {

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

    @Value("${common.test.modeling.url}")
    protected String modelingEndpointHost;

    private Model model;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        throttleConfigurationEntityMgr.deleteAll();
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path(customerBaseDir + "/INTERNAL_ModelResourceDeploymentTestNG"), true);

        LogisticRegressionAlgorithm logisticRegressionAlgorithm = new LogisticRegressionAlgorithm();
        logisticRegressionAlgorithm.setPriority(0);
        logisticRegressionAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=0");
        logisticRegressionAlgorithm.setSampleName("s0");

        DecisionTreeAlgorithm decisionTreeAlgorithm = new DecisionTreeAlgorithm();
        decisionTreeAlgorithm.setPriority(1);
        decisionTreeAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=2048 PRIORITY=1");
        decisionTreeAlgorithm.setSampleName("s1");

        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setPriority(2);
        randomForestAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=2048 PRIORITY=2");
        randomForestAlgorithm.setSampleName("all");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model Definition For Demo");
        modelDef.addAlgorithms( //
                Arrays.asList(randomForestAlgorithm, logisticRegressionAlgorithm, decisionTreeAlgorithm));

        model = new Model();
        model.setModelDefinition(modelDef);
        model.setName("Model Submission for Demo");
        model.setTable("iris");
        model.setMetadataTable("EventMetadata");
        model.setFeaturesList(Arrays.asList( //
                "SEPAL_LENGTH", //
                "SEPAL_WIDTH", //
                "PETAL_LENGTH", //
                "PETAL_WIDTH"));
        model.setTargetsList(Collections.singletonList("CATEGORY"));
        model.setCustomer("INTERNAL_ModelResourceDeploymentTestNG");
        model.setKeyCols(Collections.singletonList("ID"));
        model.setDataFormat("avro");
    }

    @Deprecated
    @Test(groups = "deployment")
    public void load() throws Exception {
        LoadConfiguration config = getLoadConfig(model);
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/load", config,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Deprecated
    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dependsOnMethods = { "load" })
    public void loadAgain() {
        LoadConfiguration config = getLoadConfig(model);
        Map<String, String> errorResult = ignoreErrorRestTemplate.postForObject(modelingEndpointHost + "/rest/load",
                config, HashMap.class);
        Assert.assertNotNull(errorResult);
        assertTrue(errorResult.containsKey("errorCode"));
    }

    @Test(groups = "deployment", dependsOnMethods = { "load" })
    public void createSamples() throws Exception {
        SamplingConfiguration samplingConfig = getSampleConfig(model);
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/createSamples",
                samplingConfig, AppSubmission.class);
        Assert.assertNotNull(submission);
        assertEquals(1, submission.getApplicationIds().size());
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "createSamples" })
    public void profile() throws Exception {
        DataProfileConfiguration config = getProfileConfig(model);
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/profile", config,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        ApplicationId profileAppId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(profileAppId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "profile" })
    public void submit() throws Exception {
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/submit", model,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        assertEquals(3, submission.getApplicationIds().size());

        for (String appIdStr : submission.getApplicationIds()) {
            ApplicationId appId = platformTestBase.getApplicationId(appIdStr);
            FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        }
    }

}
