package com.latticeengines.api.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.api.domain.AppSubmission;
import com.latticeengines.api.domain.ThrottleSubmission;
import com.latticeengines.api.functionalframework.ApiFunctionalTestNGBase;
import com.latticeengines.dataplatform.exposed.domain.Algorithm;
import com.latticeengines.dataplatform.exposed.domain.DbCreds;
import com.latticeengines.dataplatform.exposed.domain.JobStatus;
import com.latticeengines.dataplatform.exposed.domain.LoadConfiguration;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ModelDefinition;
import com.latticeengines.dataplatform.exposed.domain.SamplingConfiguration;
import com.latticeengines.dataplatform.exposed.domain.SamplingElement;
import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;
import com.latticeengines.dataplatform.exposed.domain.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.dataplatform.exposed.domain.algorithm.LogisticRegressionAlgorithm;

public class ModelResourceTestNG extends ApiFunctionalTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;
    
    private Model model;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.delete(new Path("/user/le-analytics/customers/DELL"), true);
        fs.delete(new Path("/user/le-analytics/customers/INTERNAL"), true);

        fs.mkdirs(new Path("/user/le-analytics/customers/DELL/data/DELL_EVENT_TABLE_TEST"));

        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();

        String inputDir = ClassLoader.getSystemResource("com/latticeengines/api/controller/DELL_EVENT_TABLE_TEST").getPath();
        File[] avroFiles = platformTestBase.getAvroFilesForDir(inputDir);
        for (File avroFile : avroFiles) {
            copyEntries.add(new CopyEntry("file:" + avroFile.getAbsolutePath(), "/user/le-analytics/customers/DELL/data/DELL_EVENT_TABLE_TEST", false));
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
        modelDef.setAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { decisionTreeAlgorithm,
                logisticRegressionAlgorithm }));

        model = new Model();
        model.setModelDefinition(modelDef);
        model.setName("Model Submission for Demo");
        model.setTable("DELL_EVENT_TABLE_TEST");
        model.setFeatures(Arrays.<String> asList(new String[] {
                "Column5", //
                "Column6", //
                "Column7", //
                "Column8", //
                "Column9", //
                "Column10" }));
        model.setTargets(Arrays.<String> asList(new String[] { "Event_Latitude_Customer" }));
        model.setCustomer("DELL");
        model.setDataFormat("avro");
    }

    @Test(groups = "functional", enabled = true)
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
        AppSubmission submission = restTemplate.postForObject("http://localhost:8080/rest/createSamples", samplingConfig,
                AppSubmission.class, new Object[] {});
        assertEquals(1, submission.getApplicationIds().size());
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        YarnApplicationState state = platformTestBase.waitState(appId, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
        assertEquals(state, YarnApplicationState.FINISHED);
        JobStatus status = restTemplate.getForObject("http://localhost:8080/rest/getjobstatus/" + appId.toString(), JobStatus.class, new HashMap<String, Object>());
        assertNotNull(status);
        assertEquals(status.getId(), appId.toString());
    }


    @Test(groups = "functional", enabled = true, dependsOnMethods = { "createSamples" })
    public void submit() throws Exception {
        AppSubmission submission = restTemplate.postForObject("http://localhost:8080/rest/submit", model,
                AppSubmission.class, new Object[] {});
        assertEquals(2, submission.getApplicationIds().size());
        String appId = submission.getApplicationIds().get(0);
        JobStatus status = restTemplate.getForObject("http://localhost:8080/rest/getjobstatus/" + appId, JobStatus.class, new HashMap<String, Object>());
        assertNotNull(status);
        assertEquals(status.getId(), appId);
    }

    @Test(groups = "functional", dependsOnMethods = { "submit" })
    public void throttle() throws Exception {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setImmediate(true);
        config.setJobRankCutoff(5);
        ThrottleSubmission submission = restTemplate.postForObject("http://localhost:8080/rest/throttle", config,
                ThrottleSubmission.class, new Object[] {});
        assertTrue(submission.isImmediate());
    }

    @Test(groups = "functional")
    public void load() throws Exception {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host("localhost").port(3306).db("dataplatformtest").user("root").password("welcome");
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer("INTERNAL");
        config.setTable("iris");
        AppSubmission submission = restTemplate.postForObject("http://localhost:8080/rest/load", config,
                AppSubmission.class, new Object[] {});
        ApplicationId appId = platformTestBase.getApplicationId(submission.getApplicationIds().get(0));
        YarnApplicationState state = platformTestBase.waitState(appId, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
        assertEquals(state, YarnApplicationState.FINISHED);
        JobStatus status = restTemplate.getForObject("http://localhost:8080/rest/getjobstatus/" + appId.toString(), JobStatus.class, new HashMap<String, Object>());
        assertNotNull(status);
        assertEquals(status.getId(), appId.toString());
        
    }
}
