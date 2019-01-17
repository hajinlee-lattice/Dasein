package com.latticeengines.api.controller;

import static org.testng.Assert.assertEquals;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${common.test.modeling.url}")
    protected String modelingEndpointHost;

    Model model;

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
                StringList.class, new Object[] {});
        return new AbstractMap.SimpleEntry<String, List<String>>("Target", features.getElements());
    }

    @Test(groups = "deployment", enabled = true)
    public void load() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "load");
        LoadConfiguration config = getLoadConfig(model);
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/load", config,
                AppSubmission.class, new Object[] {});
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "load" }, enabled = true)
    public void createSamples() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "createSamples");
        SamplingConfiguration samplingConfig = getSampleConfig(model);
        samplingConfig.setParallelEnabled(true);
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/createSamples",
                samplingConfig, AppSubmission.class, new Object[] {});
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
                AppSubmission.class, new Object[] {});
        ApplicationId profileAppId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(profileAppId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "profile" })
    public void submit() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "submit");
        AbstractMap.SimpleEntry<String, List<String>> targetAndFeatures = getTargetAndFeatures();
        model.setFeaturesList(targetAndFeatures.getValue());
        model.setTargetsList(Arrays.<String> asList(new String[] { targetAndFeatures.getKey() }));
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/submit", model,
                AppSubmission.class, new Object[] {});
        assertEquals(1, submission.getApplicationIds().size());

        for (String appIdStr : submission.getApplicationIds()) {
            ApplicationId appId = platformTestBase.getApplicationId(appIdStr);
            FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        }
    }
}
