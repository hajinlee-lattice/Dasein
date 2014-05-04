package com.latticeengines.api.controller;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.api.domain.AppSubmission;
import com.latticeengines.api.functionalframework.ApiFunctionalTestNGBase;
import com.latticeengines.dataplatform.exposed.domain.Algorithm;
import com.latticeengines.dataplatform.exposed.domain.DbCreds;
import com.latticeengines.dataplatform.exposed.domain.LoadConfiguration;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ModelDefinition;
import com.latticeengines.dataplatform.exposed.domain.SamplingConfiguration;
import com.latticeengines.dataplatform.exposed.domain.SamplingElement;
import com.latticeengines.dataplatform.exposed.domain.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.dataplatform.exposed.domain.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.dataplatform.exposed.domain.algorithm.RandomForestAlgorithm;

public class ModelResourceDeploymentTestNG extends ApiFunctionalTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${dataplatform.customer.basedir}")
    private String customerBaseDir;

    @Value("${api.rest.endpoint.host}")
    private String restEndpointHost;

    private Model model;

    protected boolean doYarnClusterSetup() {
        return false;
    }

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path(customerBaseDir + "/INTERNAL"), true);

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
        modelDef.setAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { randomForestAlgorithm,
                logisticRegressionAlgorithm, decisionTreeAlgorithm }));

        model = new Model();
        model.setModelDefinition(modelDef);
        model.setName("Model Submission for Demo");
        model.setTable("iris");
        model.setFeatures(Arrays.<String> asList(new String[] { "SEPAL_LENGTH", //
                "SEPAL_WIDTH", //
                "PETAL_LENGTH", //
                "PETAL_WIDTH" }));
        model.setTargets(Arrays.<String> asList(new String[] { "CATEGORY" }));
        model.setCustomer("INTERNAL");
        model.setKeyCols(Arrays.<String> asList(new String[] { "ID" }));
        model.setDataFormat("avro");
    }

    @Test(groups = "deployment")
    public void load() throws Exception {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host("localhost").port(3306).db("dataplatformtest").user("root").password("welcome");
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer("INTERNAL");
        config.setTable("iris");
        config.setKeyCols(Arrays.<String> asList(new String[] { "ID" }));
        AppSubmission submission = restTemplate.postForObject("http://" + restEndpointHost + ":8080/rest/load", config,
                AppSubmission.class, new Object[] {});
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        YarnApplicationState state = platformTestBase.waitState(appId, 120, TimeUnit.SECONDS,
                YarnApplicationState.FINISHED);
        assertEquals(state, YarnApplicationState.FINISHED);
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
        AppSubmission submission = restTemplate.postForObject(
                "http://" + restEndpointHost + ":8080/rest/createSamples", samplingConfig, AppSubmission.class,
                new Object[] {});
        assertEquals(1, submission.getApplicationIds().size());
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        YarnApplicationState state = platformTestBase.waitState(appId, 120, TimeUnit.SECONDS,
                YarnApplicationState.FINISHED);
        assertEquals(state, YarnApplicationState.FINISHED);
    }

    @Test(groups = "functional", enabled = true, dependsOnMethods = { "createSamples" })
    public void submit() throws Exception {
        AppSubmission submission = restTemplate.postForObject("http://" + restEndpointHost + ":8080/rest/submit",
                model, AppSubmission.class, new Object[] {});
        assertEquals(3, submission.getApplicationIds().size());

        for (String appIdStr : submission.getApplicationIds()) {
            ApplicationId appId = platformTestBase.getApplicationId(appIdStr);
            YarnApplicationState state = platformTestBase.waitState(appId, 120, TimeUnit.SECONDS,
                    YarnApplicationState.FINISHED);
            assertEquals(state, YarnApplicationState.FINISHED);
        }
    }

}
