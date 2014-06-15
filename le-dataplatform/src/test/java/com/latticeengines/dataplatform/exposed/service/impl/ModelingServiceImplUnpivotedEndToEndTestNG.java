package com.latticeengines.dataplatform.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.domain.exposed.dataplatform.Algorithm;
import com.latticeengines.domain.exposed.dataplatform.DbCreds;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.LoadConfiguration;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.domain.exposed.dataplatform.SamplingElement;
import com.latticeengines.domain.exposed.dataplatform.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.domain.exposed.dataplatform.algorithm.LogisticRegressionAlgorithm;

/**
 * This is an end-to-end test against a SQL Server database without having to go through the REST API.
 * It allows for an easier development-test cycle without having to either deploy to Jetty or run from le-api.
 * 
 * @author rgonzalez
 *
 */
@Transactional
public class ModelingServiceImplUnpivotedEndToEndTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private JobService jobService;

    @Autowired
    private YarnService yarnService;

    @Autowired
    private ModelingService modelingService;
    
    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private ModelEntityMgr modelEntityMgr;
    
    private Model model = null;
    
    protected boolean doYarnClusterSetup() {
        return false;
    }
    
    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {                
        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.delete(new Path("/user/s-analytics/customers/Nutanix"), true);

        LogisticRegressionAlgorithm logisticRegressionAlgorithm = new LogisticRegressionAlgorithm();
        logisticRegressionAlgorithm.setPriority(0);
        logisticRegressionAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=0");
        logisticRegressionAlgorithm.setSampleName("s0");

        DecisionTreeAlgorithm decisionTreeAlgorithm = new DecisionTreeAlgorithm();
        decisionTreeAlgorithm.setPriority(1);
        decisionTreeAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=1");
        decisionTreeAlgorithm.setSampleName("s1");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model1");
        modelDef.addAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { decisionTreeAlgorithm,
                logisticRegressionAlgorithm }));
        model = createModel(modelDef);
    }
    
    private Model createModel(ModelDefinition modelDef) {
        Model m = new Model();
        m.setModelDefinition(modelDef);
        m.setName("Model Submission1");
        m.setTable("Q_EventTable_Nutanix");
        m.setMetadataTable("EventMetadata_Nutanix");
        m.setTargetsList(Arrays.<String> asList(new String[] { "P1_Event" }));
        m.setKeyCols(Arrays.<String> asList(new String[] { "Nutanix_EventTable_Clean" }));
        m.setCustomer("Nutanix");
        m.setDataFormat("avro");
        
        return m;
    }

    private LoadConfiguration getLoadConfig() {
        LoadConfiguration config = new LoadConfiguration();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dbHost).port(dbPort).db(dbName).user(dbUser).password(dbPassword);
        DbCreds creds = new DbCreds(builder);
        config.setCreds(creds);
        config.setCustomer("Nutanix");
        config.setTable("Q_EventTable_Nutanix");
        config.setMetadataTable("EventMetadata_Nutanix");
        config.setKeyCols(Arrays.<String> asList(new String[] { "Nutanix_EventTable_Clean" }));
        return config;
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Test(groups = "functional", enabled = true)
    public void load() throws Exception {
        LoadConfiguration loadConfig = getLoadConfig();
        List<ApplicationId> loadIds = modelingService.loadData(loadConfig);
        ApplicationId appId = loadIds.get(0);
        FinalApplicationStatus status = waitForStatus(appId, 360, TimeUnit.SECONDS,
                FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }
    
    @Transactional(propagation = Propagation.REQUIRED)
    @Test(groups = "functional", enabled = true, dependsOnMethods = { "load" })
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
        ApplicationId appId = modelingService.createSamples(samplingConfig);
        FinalApplicationStatus status = waitForStatus(appId, 240, TimeUnit.SECONDS,
                FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Test(groups = "functional", enabled = true, dependsOnMethods = { "createSamples" })
    public void submitModel() throws Exception {
        List<String> features = modelingService.getFeatures(model, false);
        model.setFeaturesList(features);
        
        List<ApplicationId> appIds = modelingService.submitModel(model);

        for (ApplicationId appId : appIds) {
            YarnApplicationState state = waitState(appId, 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
            assertNotNull(state);
            FinalApplicationStatus status = waitForStatus(appId, 300, TimeUnit.SECONDS,
                    FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);

            JobStatus jobStatus = modelingService.getJobStatus(appId.toString());
            String modelFile = HdfsUtils.getFilesForDir(yarnConfiguration, jobStatus.getResultDirectory()).get(0);
            String modelContents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelFile);
            assertNotNull(modelContents);
        }
    }

}

