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

import com.latticeengines.api.functionalframework.ApiFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

public class NutanixDeploymentTestNG extends ApiFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(NutanixDeploymentTestNG.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${common.test.modeling.url}")
    protected String modelingEndpointHost;

    private Model model;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path(customerBaseDir + "/INTERNAL_NutanixDeploymentTestNG"), true);

        ModelDefinition modelDef = produceModelDef();
        this.model = produceAModel(modelDef);
    }

    protected Model produceAModel(ModelDefinition modelDef) {
        Model model = new Model();
        model.setModelDefinition(modelDef);
        model.setName("INTERNAL_NutanixDeploymentTestNG Random Forest Model on Depivoted Data");
        model.setTable("Q_EventTable_Nutanix");
        model.setMetadataTable("EventMetadata");
        model.setCustomer("INTERNAL_NutanixDeploymentTestNG");
        model.setKeyCols(Arrays.<String> asList(new String[] { "Nutanix_EventTable_Clean" }));
        model.setDataFormat("avro");

        return model;
    }

    protected ModelDefinition produceModelDef() {
        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setPriority(0);
        randomForestAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=2048 PRIORITY=2");
        randomForestAlgorithm.setSampleName("all");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Logistic regression against all");
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { randomForestAlgorithm }));
        return modelDef;
    }

    private AbstractMap.SimpleEntry<String, List<String>> getTargetAndFeatures() {
        StringList features = restTemplate.postForObject(modelingEndpointHost + "/rest/features", model,
                StringList.class, new Object[] {});
        return new AbstractMap.SimpleEntry<String, List<String>>("P1_Event", features.getElements());
    }

    @Test(groups = "deployment", enabled = true)
    public void load() throws Exception {
        LoadConfiguration config = getLoadConfig();
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/load", config,
                AppSubmission.class, new Object[] {});
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    private LoadConfiguration getLoadConfig() {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dataSourceHost).port(dataSourcePort).db(dataSourceDB).user(dataSourceUser)
                .clearTextPassword(dataSourcePasswd).dbType(dataSourceDBType);
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer("INTERNAL_NutanixDeploymentTestNG");
        config.setTable("Q_EventTable_Nutanix");
        config.setKeyCols(Arrays.<String> asList(new String[] { "Nutanix_EventTable_Clean" }));
        return config;
    }

    @Test(groups = "deployment", dependsOnMethods = { "load" }, enabled = true)
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
        samplingConfig.setCustomer(this.model.getCustomer());
        samplingConfig.setTable(this.model.getTable());

        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/createSamples",
                samplingConfig, AppSubmission.class, new Object[] {});
        assertEquals(1, submission.getApplicationIds().size());
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "createSamples" })
    public void profile() throws Exception {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(model.getCustomer());
        config.setTable(model.getTable());
        config.setMetadataTable(model.getMetadataTable());
        config.setSamplePrefix("all");
        config.setExcludeColumnList(ModelingServiceTestUtils.createExcludeList());
        config.setTargets(Arrays.<String> asList(new String[] { "P1_Event" }));
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/profile", config,
                AppSubmission.class, new Object[] {});
        ApplicationId profileAppId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(profileAppId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "profile" })
    public void submit() throws Exception {
        AbstractMap.SimpleEntry<String, List<String>> targetAndFeatures = getTargetAndFeatures();
        this.model.setFeaturesList(targetAndFeatures.getValue());
        this.model.setTargetsList(Arrays.<String> asList(new String[] { targetAndFeatures.getKey() }));
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/submit", this.model,
                AppSubmission.class, new Object[] {});
        assertEquals(1, submission.getApplicationIds().size());

        for (String appIdStr : submission.getApplicationIds()) {
            ApplicationId appId = platformTestBase.getApplicationId(appIdStr);
            FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        }
    }

}
