package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;

public class ModelingServiceImplParallelProfilingSingleModelingTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private ModelingService modelingService;

    private Model model;

    private DbCreds getCreds() {
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dataSourceHost).port(dataSourcePort).db(dataSourceDB).user(dataSourceUser)
                .clearTextPassword(dataSourcePasswd).dbType(dataSourceDBType);
        return new DbCreds(builder);
    }

    private LoadConfiguration getLoadConfig() {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds creds = getCreds();
        config.setCreds(creds);
        config.setCustomer(getCustomer());
        config.setTable("Q_EventTable_Nutanix");
        config.setMetadataTable("EventMetadata");
        config.setKeyCols(Arrays.<String> asList(new String[] { "Nutanix_EventTable_Clean" }));
        return config;
    }

    private String getCustomer() {
        return "Nutanix";
    }

    private Model createModel(ModelDefinition modelDef) {
        Model m = new Model();
        m.setModelDefinition(modelDef);
        m.setName("Model Submission1");
        m.setTable("Q_EventTable_Nutanix");
        m.setMetadataTable("Q_EventTable_Nutanix-EventMetadata");
        m.setTargetsList(Arrays.<String> asList(new String[] { "P1_Event" }));
        m.setKeyCols(Arrays.<String> asList(new String[] { "Nutanix_EventTable_Clean" }));
        m.setCustomer(getCustomer());
        m.setDataFormat("avro");
        return m;
    }

    @BeforeClass(groups = "sqoop")
    public void setup() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        String customer = getCustomer();
        fs.delete(new Path(String.format("%s/%s", customerBaseDir, customer)), true);
        fs.delete(new Path(String.format("%s/%s.%s.Production", customerBaseDir, customer, customer)), true);

        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setPriority(0);
        randomForestAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=0");
        randomForestAlgorithm.setSampleName("all");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model1");
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { randomForestAlgorithm }));

        model = createModel(modelDef);
    }


    @Test(groups = "sqoop")
    public void loadData() throws Exception {
        LoadConfiguration loadConfig = getLoadConfig();
        ApplicationId appId = modelingService.loadData(loadConfig);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "sqoop", dependsOnMethods = { "loadData" })
    public void createSamples() throws Exception {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setRandomSeed(123456L);
        samplingConfig.setTrainingPercentage(80);
        SamplingElement s0 = new SamplingElement();
        s0.setName("s0");
        s0.setPercentage(20);
        SamplingElement all = new SamplingElement();
        all.setName("all");
        all.setPercentage(100);
        samplingConfig.addSamplingElement(s0);
        samplingConfig.addSamplingElement(all);
        samplingConfig.setCustomer(model.getCustomer());
        samplingConfig.setTable(model.getTable());
        samplingConfig.setParallelEnabled(true);
        ApplicationId appId = modelingService.createSamples(samplingConfig);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "sqoop", dependsOnMethods = { "createSamples" })
    public void profile() throws Exception {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(model.getCustomer());
        config.setTable(model.getTable());
        config.setMetadataTable(model.getMetadataTable());
        config.setExcludeColumnList(ModelingServiceTestUtils.createExcludeList());
        config.setSamplePrefix("all");
        config.setTargets(model.getTargetsList());
        config.setParallelEnabled(true);
        ApplicationId appId = modelingService.profileData(config);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "sqoop", dependsOnMethods = { "profile" })
    public void submit() throws Exception {
        List<String> features = modelingService.getFeatures(model, false);
        model.setFeaturesList(features);

        List<ApplicationId> appIds = modelingService.submitModel(model);

        for (ApplicationId appId : appIds) {
            FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);

            JobStatus jobStatus = modelingService.getJobStatus(appId.toString());
            String modelFile = HdfsUtils.getFilesForDir(yarnConfiguration, jobStatus.getResultDirectory()).get(0);
            String modelContents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelFile);
            assertNotNull(modelContents);
        }
    }
}
