package com.latticeengines.api.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.api.domain.AppSubmission;
import com.latticeengines.api.domain.ThrottleSubmission;
import com.latticeengines.api.functionalframework.ApiFunctionalTestNGBase;
import com.latticeengines.dataplatform.exposed.domain.Algorithm;
import com.latticeengines.dataplatform.exposed.domain.DataSchema;
import com.latticeengines.dataplatform.exposed.domain.Field;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ModelDefinition;
import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;
import com.latticeengines.dataplatform.exposed.domain.algorithm.AlgorithmBase;
import com.latticeengines.dataplatform.exposed.domain.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.dataplatform.exposed.domain.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.dataplatform.util.HdfsHelper;
import com.latticeengines.dataplatform.util.JsonHelper;

public class ModelResourceTestNG extends ApiFunctionalTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.delete(new Path("/apitest"), true);

        fs.mkdirs(new Path("/apitest"));

        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();

        String trainingFilePath = platformTestBase
                .getFileUrlFromResource("com/latticeengines/api/controller/train.dat");
        String testFilePath = platformTestBase.getFileUrlFromResource("com/latticeengines/api/controller/test.dat");
        String jsonFilePath = platformTestBase.getFileUrlFromResource("com/latticeengines/api/controller/iris.json");
        String logisticRegressionPythonScriptPath = platformTestBase
                .getFileUrlFromResource("com/latticeengines/api/controller/lr_train.py");
        String decisionTreePythonScriptPath = platformTestBase
                .getFileUrlFromResource("com/latticeengines/api/controller/dt_train.py");

        copyEntries.add(new CopyEntry(trainingFilePath, "/apitest", false));
        copyEntries.add(new CopyEntry(testFilePath, "/apitest", false));
        copyEntries.add(new CopyEntry(jsonFilePath, "/apitest", false));
        copyEntries.add(new CopyEntry(decisionTreePythonScriptPath, "/apitest", false));
        copyEntries.add(new CopyEntry(logisticRegressionPythonScriptPath, "/apitest", false));

        platformTestBase.doCopy(fs, copyEntries);

    }

    @Test(groups = "functional", enabled = true)
    public void submit() throws Exception {
        AlgorithmBase decisionTreeAlgorithm = new DecisionTreeAlgorithm();
        decisionTreeAlgorithm.setName("DT");
        decisionTreeAlgorithm.setScript("/apitest/dt_train.py");
        decisionTreeAlgorithm.setPriority(1);
        decisionTreeAlgorithm.setContainerProperties("MEMORY=2048 VIRTUALCORES=1 PRIORITY=0");

        AlgorithmBase logisticRegressionAlgorithm = new LogisticRegressionAlgorithm();
        logisticRegressionAlgorithm.setName("LR");
        logisticRegressionAlgorithm.setScript("/apitest/lr_train.py");
        logisticRegressionAlgorithm.setPriority(0);
        logisticRegressionAlgorithm.setContainerProperties("MEMORY=2048 VIRTUALCORES=1 PRIORITY=0");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model Definition For Demo");
        modelDef.setAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { decisionTreeAlgorithm,
                logisticRegressionAlgorithm }));

        Model model = new Model();
        model.setModelDefinition(modelDef);
        model.setName("Model Submission for Demo");
        model.setSchemaHdfsPath("/apitest/iris.json");
        model.setTrainingDataHdfsPath("/apitest/train.dat");
        model.setTestDataHdfsPath("/apitest/test.dat");
        model.setModelHdfsDir("/apitest/model");

        String metadataJson = HdfsHelper.getHdfsFileContents(yarnConfiguration, "/apitest/iris.json");
        DataSchema schema = JsonHelper.deserialize(metadataJson, DataSchema.class);

        List<String> features = new ArrayList<String>();

        Set<String> nonFeatureFields = new HashSet<String>();

        nonFeatureFields.addAll(Arrays.<String> asList(new String[] { "category" }));

        for (Field field : schema.getFields()) {
            if (!nonFeatureFields.contains(field.getName())) {
                features.add(field.getName());
            }
        }
        model.setFeatures(features);
        model.setTargets(Arrays.<String> asList(new String[] { "category" }));
        model.setQueue("Priority0.A");

        AppSubmission submission = restTemplate.postForObject("http://localhost:8080/rest/submit", model,
                AppSubmission.class, new Object[] {});
        assertEquals(2, submission.getApplicationIds().size());
    }

    @Test(groups = "functional")
    public void throttle() throws Exception {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setImmediate(true);
        config.setJobRankCutoff(5);
        ThrottleSubmission submission = restTemplate.postForObject("http://localhost:8080/rest/throttle", config,
                ThrottleSubmission.class, new Object[] {});
        assertTrue(submission.isImmediate());
    }

}
