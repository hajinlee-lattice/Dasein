package com.latticeengines.api.controller;

import static org.testng.Assert.assertEquals;

import java.net.URL;
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
import com.latticeengines.api.functionalframework.ApiFunctionalTestNGBase;
import com.latticeengines.dataplatform.exposed.domain.Algorithm;
import com.latticeengines.dataplatform.exposed.domain.DataSchema;
import com.latticeengines.dataplatform.exposed.domain.Field;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ModelDefinition;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.util.HdfsHelper;
import com.latticeengines.dataplatform.util.JsonHelper;

public class ModelResourceTestNG extends ApiFunctionalTestNGBase {

    @Autowired
    private Configuration yarnConfiguration;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.delete(new Path("/training"), true);
        fs.delete(new Path("/test"), true);
        fs.delete(new Path("/apitest"), true);

        fs.mkdirs(new Path("/training"));
        fs.mkdirs(new Path("/test"));
        fs.mkdirs(new Path("/apitest"));

        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
        URL trainingFileUrl = ClassLoader.getSystemResource("com/latticeengines/api/controller/train.dat");
        URL testFileUrl = ClassLoader.getSystemResource("com/latticeengines/api/controller/test.dat");
        URL jsonUrl = ClassLoader.getSystemResource("com/latticeengines/api/controller/iris.json");
        URL decisionTreePythonScriptUrl = ClassLoader
                .getSystemResource("com/latticeengines/api/controller/dt_train.py");
        URL logisticRegressionPythonScriptUrl = ClassLoader
                .getSystemResource("com/latticeengines/api/controller/lr_train.py");

        String trainingFilePath = "file:" + trainingFileUrl.getFile();
        String testFilePath = "file:" + testFileUrl.getFile();
        String jsonFilePath = "file:" + jsonUrl.getFile();
        String decisionTreePythonScriptPath = "file:" + decisionTreePythonScriptUrl.getFile();
        String logisticRegressionPythonScriptPath = "file:" + logisticRegressionPythonScriptUrl.getFile();

        copyEntries.add(new CopyEntry(trainingFilePath, "/training", false));
        copyEntries.add(new CopyEntry(testFilePath, "/test", false));
        copyEntries.add(new CopyEntry(jsonFilePath, "/apitest", false));
        copyEntries.add(new CopyEntry(decisionTreePythonScriptPath, "/apitest", false));
        copyEntries.add(new CopyEntry(logisticRegressionPythonScriptPath, "/apitest", false));

        DataPlatformFunctionalTestNGBase platformTestBase = new DataPlatformFunctionalTestNGBase(yarnConfiguration);
        platformTestBase.doCopy(fs, copyEntries);

    }

    @Test(groups = "functional")
    public void submit() throws Exception {
        Algorithm decisionTreeAlgorithm = new Algorithm();
        decisionTreeAlgorithm.setName("DT");
        decisionTreeAlgorithm.setScript("/demo/dt_train.py");
        decisionTreeAlgorithm.setPriority(1);
        decisionTreeAlgorithm.setContainerProperties("MEMORY=2048 VIRTUALCORES=1 PRIORITY=0");

        Algorithm logisticRegressionAlgorithm = new Algorithm();
        logisticRegressionAlgorithm.setName("LR");
        logisticRegressionAlgorithm.setScript("/demo/lr_train.py");
        logisticRegressionAlgorithm.setPriority(0);
        logisticRegressionAlgorithm.setContainerProperties("MEMORY=2048 VIRTUALCORES=1 PRIORITY=0");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model Definition For Demo");
        modelDef.setAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { decisionTreeAlgorithm,
                logisticRegressionAlgorithm }));

        Model model = new Model();
        model.setModelDefinition(modelDef);
        model.setName("Model Submission for Demo");
        model.setSchemaHdfsPath("/demo/metadata.json");
        model.setTrainingDataHdfsPath("/demo/train.dat");
        model.setTestDataHdfsPath("/demo/test.dat");
        model.setModelHdfsDir("/demo/model");

        String metadataJson = HdfsHelper.getHdfsFileContents(yarnConfiguration, "/demo/metadata.json");
        DataSchema schema = JsonHelper.deserialize(metadataJson, DataSchema.class);

        List<String> features = new ArrayList<String>();

        Set<String> nonFeatureFields = new HashSet<String>();

        nonFeatureFields.addAll(Arrays.<String> asList(new String[] { //
                "Event_Latitude_Customer", //
                        "CustomerID", //
                        "PeriodID", //
                        "IDX", //
                        "Target_LatitudeOptiplex_Retention_PCA_PPA_Customer", //
                        "TargetTraining_LatitudeOptiplex_Retention_PCA_PPA_Customer" //
                }));

        for (Field field : schema.getFields()) {
            if (!nonFeatureFields.contains(field.getName())) {
                features.add(field.getName());
            }
        }
        model.setFeatures(features);
        model.setTargets(Arrays.<String> asList(new String[] { "Event_Latitude_Customer" }));
        model.setQueue("Dell");

        AppSubmission submission = restTemplate.postForObject("http://localhost:8080/rest/submit", model,
                AppSubmission.class, new Object[] {});
        assertEquals(2, submission.getApplicationIds().size());
    }
}
