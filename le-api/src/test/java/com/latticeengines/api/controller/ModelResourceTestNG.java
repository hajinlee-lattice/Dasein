package com.latticeengines.api.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.api.functionalframework.ApiFunctionalTestNGBase;
import com.latticeengines.dataplatform.entitymanager.modeling.ThrottleConfigurationEntityMgr;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.ThrottleSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;
import com.latticeengines.domain.exposed.modeling.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.modeling.algorithm.LogisticRegressionAlgorithm;

public class ModelResourceTestNG extends ApiFunctionalTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

    private Model model;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        throttleConfigurationEntityMgr.deleteAll();
        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.delete(new Path(customerBaseDir + "/DELL"), true);
        fs.delete(new Path(customerBaseDir + "/INTERNAL"), true);

        fs.mkdirs(new Path(customerBaseDir + "/DELL/data/DELL_EVENT_TABLE_TEST"));

        List<CopyEntry> copyEntries = new ArrayList<>();

        String inputDir = ClassLoader.getSystemResource("com/latticeengines/api/controller/DELL_EVENT_TABLE_TEST")
                .getPath();
        File[] avroFiles = platformTestBase.getAvroFilesForDir(inputDir);
        for (File avroFile : avroFiles) {
            copyEntries.add(new CopyEntry("file:" + avroFile.getAbsolutePath(), customerBaseDir
                    + "/DELL/data/DELL_EVENT_TABLE_TEST", false));
        }

        LogisticRegressionAlgorithm logisticRegressionAlgorithm = new LogisticRegressionAlgorithm();
        logisticRegressionAlgorithm.setPriority(0);
        logisticRegressionAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=0");
        logisticRegressionAlgorithm.setSampleName("s0");

        DecisionTreeAlgorithm decisionTreeAlgorithm = new DecisionTreeAlgorithm();
        decisionTreeAlgorithm.setPriority(1);
        decisionTreeAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=1");
        decisionTreeAlgorithm.setSampleName("s1");
        platformTestBase.doCopy(fs, copyEntries);

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model Definition For Demo");
        modelDef.addAlgorithms(Arrays.asList(decisionTreeAlgorithm, logisticRegressionAlgorithm));

        model = new Model();
        model.setModelDefinition(modelDef);
        model.setName("Model Submission for Demo");
        model.setTable("DELL_EVENT_TABLE_TEST");
        model.setMetadataTable("EventMetadata");
        model.setFeaturesList(Arrays.asList( //
                "Column5", //
                "Column6", //
                "Column7", //
                "Column8", //
                "Column9", //
                "Column10"));
        model.setTargetsList(Collections.singletonList("Event_Latitude_Customer"));
        model.setCustomer("DELL");
        model.setKeyCols(Collections.singletonList("IDX"));
        model.setDataFormat("avro");
    }

    @Test(groups = "functional")
    public void createSamples() throws Exception {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        SamplingElement s0 = new SamplingElement();
        s0.setName("s0");
        s0.setPercentage(30);
        SamplingElement s1 = new SamplingElement();
        s1.setName("s1");
        s1.setPercentage(60);
        SamplingElement s2 = new SamplingElement();
        s2.setName("all");
        s2.setPercentage(100);
        samplingConfig.addSamplingElement(s0);
        samplingConfig.addSamplingElement(s1);
        samplingConfig.addSamplingElement(s2);
        samplingConfig.setCustomer(model.getCustomer());
        samplingConfig.setTable(model.getTable());
        AppSubmission submission = restTemplate.postForObject("http://localhost:8080/rest/createSamples",
                samplingConfig, AppSubmission.class);
        Assert.assertNotNull(submission);
        assertEquals(1, submission.getApplicationIds().size());
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        validateAppStatus(appId);
    }

    @Test(groups = "functional", dependsOnMethods = { "createSamples" })
    public void profile() throws Exception {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(model.getCustomer());
        config.setTable(model.getTable());
        config.setMetadataTable(model.getMetadataTable());
        config.setIncludeColumnList(model.getFeaturesList());
        config.setSamplePrefix("all");
        config.setTargets(model.getTargetsList());
        AppSubmission submission = restTemplate.postForObject("http://localhost:8080/rest/profile", config,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        ApplicationId profileAppId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = platformTestBase.waitForStatus(profileAppId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "functional", dependsOnMethods = { "profile" })
    public void submit() {
        // reset throttle
        restTemplate.postForObject("http://localhost:8080/rest/resetThrottle", null, ThrottleSubmission.class);
        // submit
        AppSubmission submission = restTemplate.postForObject("http://localhost:8080/rest/submit", model,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        assertEquals(2, submission.getApplicationIds().size());
        String appId = submission.getApplicationIds().get(0);
        validateAppStatus(platformTestBase.getApplicationId(appId));
    }

    @Test(groups = "functional", dependsOnMethods = { "submit" })
    public void throttle() {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setImmediate(true);
        config.setJobRankCutoff(5);
        ThrottleSubmission submission = restTemplate.postForObject("http://localhost:8080/rest/throttle", config,
                ThrottleSubmission.class);
        Assert.assertNotNull(submission);
        assertTrue(submission.isImmediate());
    }

    @Test(groups = "functional", enabled = false)
    public void load() throws Exception {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dataSourceHost) //
                .port(dataSourcePort) //
                .db(dataSourceDB) //
                .user(dataSourceUser) //
                .clearTextPassword(dataSourcePasswd)//
                .dbType(dataSourceDBType);
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer("INTERNAL");
        config.setTable("iris");
        config.setMetadataTable("EventMetadata");
        config.setKeyCols(Collections.singletonList("ID"));
        AppSubmission submission = restTemplate.postForObject("http://localhost:8080/rest/load", config,
                AppSubmission.class);
        Assert.assertNotNull(submission);
        ApplicationId metadataLoadAppId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus status = waitForStatus(metadataLoadAppId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        validateAppStatus(metadataLoadAppId);
    }

    private void validateAppStatus(ApplicationId appId) {
        JobStatus status = restTemplate.getForObject("http://localhost:8080/rest/getJobStatus/" + appId.toString(),
                JobStatus.class, new HashMap<>());
        assertNotNull(status);
        assertEquals(status.getId(), appId.toString());
    }

}
