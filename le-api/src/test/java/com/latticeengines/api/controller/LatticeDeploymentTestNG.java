package com.latticeengines.api.controller;

import static org.testng.Assert.assertEquals;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;

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

import com.latticeengines.api.functionalframework.ApiFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

@Deprecated
public class LatticeDeploymentTestNG extends ApiFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(LatticeDeploymentTestNG.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${common.test.modeling.url}")
    protected String modelingEndpointHost;

    private Model model;

    @BeforeClass(groups = "deployment", enabled = false)
    public void setup() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path(customerBaseDir + "/INTERNAL_LatticeDeploymentTestNG"), true);

        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setPriority(0);
        randomForestAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=2048 PRIORITY=2");
        randomForestAlgorithm.setSampleName("all");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Random Forest against all");
        modelDef.addAlgorithms(Collections.singletonList(randomForestAlgorithm));

        model = new Model();
        model.setModelDefinition(modelDef);
        model.setName("INTERNAL_LatticeDeploymentTestNG Random Forest Model on raw Data");
        model.setTable("DataForScoring_Lattice");
        model.setMetadataTable("EventMetadata");
        model.setCustomer("INTERNAL_LatticeDeploymentTestNG");
        model.setKeyCols(Collections.singletonList("LeadID"));
        model.setDataFormat("avro");
    }

    private AbstractMap.SimpleEntry<String, List<String>> getTargetAndFeatures() {
        StringList features = restTemplate.postForObject(modelingEndpointHost + "/rest/features", model,
                StringList.class);
        Assert.assertNotNull(features);
        return new AbstractMap.SimpleEntry<>("P1_Event", features.getElements());
    }

    @Test(groups = "deployment", enabled = false)
    public void load() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "load");
        LoadConfiguration config = getLoadConfig();
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/load", config,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED, String.format("ApplicationId is %s", appId.toString()));
    }

    private LoadConfiguration getLoadConfig() {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dataSourceHost).port(dataSourcePort).db(dataSourceDB).user(dataSourceUser)
                .clearTextPassword(dataSourcePasswd).dbType(dataSourceDBType);
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer("INTERNAL_LatticeDeploymentTestNG");
        config.setTable("DataForScoring_Lattice");
        config.setKeyCols(Collections.singletonList("LeadID"));
        return config;
    }

    @Test(groups = "deployment", dependsOnMethods = { "load" }, enabled = false)
    public void createSamples() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "createSamples");
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

        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/createSamples",
                samplingConfig, AppSubmission.class);
        Assert.assertNotNull(submission);
        assertEquals(1, submission.getApplicationIds().size());
        Assert.assertNotNull(submission);
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED, String.format("ApplicationId is %s", appId.toString()));
    }

    @Test(groups = "deployment", dependsOnMethods = { "createSamples" }, enabled = false)
    public void profile() throws Exception {
        log.info("               info..............." + this.getClass().getSimpleName() + "profile");
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(model.getCustomer());
        config.setTable(model.getTable());
        config.setMetadataTable(model.getMetadataTable());
        config.setSamplePrefix("all");
        config.setTargets(Collections.singletonList("P1_Event"));
        config.setExcludeColumnList(ModelingServiceTestUtils.createExcludeList());
        AppSubmission submission = restTemplate.postForObject(modelingEndpointHost + "/rest/profile", config,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        ApplicationId profileAppId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(profileAppId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED,
                String.format("ApplicationId is %s", profileAppId.toString()));
    }

    @Test(groups = "deployment", dependsOnMethods = { "profile" }, enabled = false)
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
            assertEquals(status, FinalApplicationStatus.SUCCEEDED,
                    String.format("ApplicationId is %s", appId.toString()));
        }
    }

}
