package com.latticeengines.api.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.api.functionalframework.ApiFunctionalTestNGBase;
import com.latticeengines.dataplatform.entitymanager.modeling.ThrottleConfigurationEntityMgr;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

public class ModelResourceDeploymentTestNG extends ApiFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ModelResourceDeploymentTestNG.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

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
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { randomForestAlgorithm,
                logisticRegressionAlgorithm, decisionTreeAlgorithm }));

        model = new Model();
        model.setModelDefinition(modelDef);
        model.setName("Model Submission for Demo");
        model.setTable("iris");
        model.setMetadataTable("EventMetadata");
        model.setFeaturesList(Arrays.<String> asList(new String[] { "SEPAL_LENGTH", //
                "SEPAL_WIDTH", //
                "PETAL_LENGTH", //
                "PETAL_WIDTH" }));
        model.setTargetsList(Arrays.<String> asList(new String[] { "CATEGORY" }));
        model.setCustomer("INTERNAL_ModelResourceDeploymentTestNG");
        model.setKeyCols(Arrays.<String> asList(new String[] { "ID" }));
        model.setDataFormat("avro");
    }

    @Test(groups = "deployment")
    public void load() throws Exception {
        LoadConfiguration config = getLoadConfig();
        AppSubmission submission = restTemplate.postForObject("http://" + restEndpointHost + "/rest/load", config,
                AppSubmission.class, new Object[] {});
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment", dependsOnMethods = { "load" })
    public void loadAgain() throws Exception {
        LoadConfiguration config = getLoadConfig();
        Map<String, String> errorResult = ignoreErrorRestTemplate.postForObject("http://" + restEndpointHost
                + "/rest/load", config, HashMap.class, new Object[] {});
        assertTrue(errorResult.get("errorCode").equals("LEDP_12010"));
    }

    private LoadConfiguration getLoadConfig() {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dataSourceHost) //
                .port(dataSourcePort) //
                .db(dataSourceDB) //
                .user(dataSourceUser) //
                .password(dataSourcePasswd) //
                .dbType(dataSourceDBType);
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer("INTERNAL_ModelResourceDeploymentTestNG");
        config.setTable("iris");
        config.setKeyCols(Arrays.<String> asList(new String[] { "ID" }));
        config.setMetadataTable("EventMetadata");
        return config;
    }

    @Test(groups = "deployment", dependsOnMethods = { "load" })
    public void createSamples() throws Exception {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        SamplingElement s0 = new SamplingElement();
        s0.setName("s0");
        s0.setPercentage(30);
        SamplingElement s1 = new SamplingElement();
        s1.setName("s1");
        s1.setPercentage(60);
        SamplingElement all = new SamplingElement();
        all.setName("all");
        all.setPercentage(100);
        samplingConfig.addSamplingElement(s0);
        samplingConfig.addSamplingElement(s1);
        samplingConfig.addSamplingElement(all);
        samplingConfig.setCustomer(model.getCustomer());
        samplingConfig.setTable(model.getTable());
        AppSubmission submission = restTemplate.postForObject("http://" + restEndpointHost + "/rest/createSamples",
                samplingConfig, AppSubmission.class, new Object[] {});
        assertEquals(1, submission.getApplicationIds().size());
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "createSamples" })
    public void profile() throws Exception {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(model.getCustomer());
        config.setTable(model.getTable());
        config.setMetadataTable(model.getMetadataTable());
        config.setIncludeColumnList(model.getFeaturesList());
        config.setSamplePrefix("all");
        config.setTargets(model.getTargetsList());
        AppSubmission submission = restTemplate.postForObject("http://" + restEndpointHost + "/rest/profile", config,
                AppSubmission.class, new Object[] {});
        ApplicationId profileAppId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(profileAppId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "profile" })
    public void submit() throws Exception {
        AppSubmission submission = restTemplate.postForObject("http://" + restEndpointHost + "/rest/submit", model,
                AppSubmission.class, new Object[] {});
        assertEquals(3, submission.getApplicationIds().size());

        for (String appIdStr : submission.getApplicationIds()) {
            ApplicationId appId = platformTestBase.getApplicationId(appIdStr);
            FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        }
    }

}
