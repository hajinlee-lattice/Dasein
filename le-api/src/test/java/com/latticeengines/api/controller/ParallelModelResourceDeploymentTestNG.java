package com.latticeengines.api.controller;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

public class ParallelModelResourceDeploymentTestNG extends BaseModelResourceDeploymentTestNG {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ParallelModelResourceDeploymentTestNG.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

    @Value("${common.test.modeling.url}")
    protected String modelingEndpointHost;

    private Model model;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        throttleConfigurationEntityMgr.deleteAll();
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path(customerBaseDir + "/Parallel_INTERNAL_ModelResourceDeploymentTestNG"), true);

        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setScript("/datascience/dataplatform/scripts/algorithm/parallel_rf_train.py");
        randomForestAlgorithm.setPriority(2);
        randomForestAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=2048 PRIORITY=2");
        randomForestAlgorithm.setSampleName("all");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model Definition For Demo");
        modelDef.addAlgorithms(Collections.singletonList(randomForestAlgorithm));

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
        model.setCustomer("Parallel_INTERNAL_ModelResourceDeploymentTestNG");
        model.setKeyCols(Collections.singletonList("ID"));
        model.setDataFormat("avro");
    }

    @Deprecated
    @Test(groups = "deployment")
    public void parallel_load() throws Exception {
        LoadConfiguration config = getLoadConfig(model);
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/load", config,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "parallel_load" })
    public void parallel_createSamples() throws Exception {
        SamplingConfiguration samplingConfig = getSampleConfig(model);
        samplingConfig.setParallelEnabled(true);

        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/createSamples",
                samplingConfig, AppSubmission.class);
        Assert.assertNotNull(submission);
        assertEquals(1, submission.getApplicationIds().size());
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "parallel_createSamples" })
    public void parallel_profile() throws Exception {
        DataProfileConfiguration config = getProfileConfig(model);
        config.setParallelEnabled(true);

        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/profile", config,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        ApplicationId profileAppId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(profileAppId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "parallel_profile" })
    public void submit() throws Exception {
        model.setParallelEnabled(true);
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/submit", model,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        assertEquals(1, submission.getApplicationIds().size());

        for (String appIdStr : submission.getApplicationIds()) {
            ApplicationId appId = platformTestBase.getApplicationId(appIdStr);
            FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        }
    }

}
