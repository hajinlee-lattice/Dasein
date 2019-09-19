package com.latticeengines.api.controller;

import static org.testng.Assert.assertEquals;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

public class ParallelDellAPJDeploymentTestNG extends BaseDellAPJDeploymentTestNG {

    private static final Logger log = LoggerFactory.getLogger(ParallelDellAPJDeploymentTestNG.class);

    @Inject
    private Configuration yarnConfiguration;

    @Value("${common.test.modeling.url}")
    protected String modelingEndpointHost;

    private Model model;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {

        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path(customerBaseDir + "/Parallel_INTERNAL_DellAPJDeploymentTestNG"), true);

        model = getModel("Parallel_INTERNAL_DellAPJDeploymentTestNG");
        RandomForestAlgorithm randomForestAlgorithm = (RandomForestAlgorithm) model.getModelDefinition().getAlgorithms()
                .get(0);
        randomForestAlgorithm.setScript("/datascience/dataplatform/scripts/algorithm/parallel_rf_train.py");
        model.setParallelEnabled(true);
    }

    private AbstractMap.SimpleEntry<String, List<String>> getTargetAndFeatures() {
        StringList features = restTemplate.postForObject(modelingEndpointHost + "/rest/features", model,
                StringList.class);
        Assert.assertNotNull(features);
        return new AbstractMap.SimpleEntry<>("Target", features.getElements());
    }

    @Deprecated
    @Test(groups = "deployment")
    public void load() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "load");
        LoadConfiguration config = getLoadConfig(model);
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/load", config,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "load" })
    public void createSamples() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "createSamples");
        SamplingConfiguration samplingConfig = getSampleConfig(model);
        samplingConfig.setParallelEnabled(true);
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/createSamples",
                samplingConfig, AppSubmission.class);
        Assert.assertNotNull(submission);
        assertEquals(1, submission.getApplicationIds().size());
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "createSamples" })
    public void profile() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "profile");
        DataProfileConfiguration config = getProfileConfig(model);
        config.setParallelEnabled(true);
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/profile", config,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        ApplicationId profileAppId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(profileAppId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "profile" })
    public void submit() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "submit");
        AbstractMap.SimpleEntry<String, List<String>> targetAndFeatures = getTargetAndFeatures();
        model.setFeaturesList(targetAndFeatures.getValue());
        model.setTargetsList(Collections.singletonList(targetAndFeatures.getKey()));
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/submit", model,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        assertEquals(1, submission.getApplicationIds().size());

        for (String appIdStr : submission.getApplicationIds()) {
            ApplicationId appId = platformTestBase.getApplicationId(appIdStr);
            FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        }
    }
}
