package com.latticeengines.dataplatform.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.impl.JobEntityMgrImpl;
import com.latticeengines.dataplatform.entitymanager.impl.ModelEntityMgrImpl;
import com.latticeengines.dataplatform.entitymanager.impl.ThrottleConfigurationEntityMgrImpl;
import com.latticeengines.dataplatform.exposed.domain.Algorithm;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ModelDefinition;
import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.JobWatchdogService;
import com.latticeengines.dataplatform.service.impl.JobWatchdogServiceImpl;
import com.latticeengines.dataplatform.util.HdfsHelper;

public class ModelingServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private JobService jobService;

    @Autowired
    private YarnService yarnService;

    @Autowired
    private ModelingService modelingService;
    
    @Autowired
    private JobEntityMgrImpl jobEntityMgr;

    @Autowired
    private ModelEntityMgrImpl modelEntityMgr;
    
    @Autowired
    private ThrottleConfigurationEntityMgrImpl throttleConfigurationEntityMgr;
    
    private Model model = null;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        modelEntityMgr.deleteStoreFile();
        throttleConfigurationEntityMgr.deleteStoreFile();
        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.delete(new Path("/training"), true);
        fs.delete(new Path("/test"), true);
        fs.delete(new Path("/datascientist2"), true);

        fs.mkdirs(new Path("/training"));
        fs.mkdirs(new Path("/test"));
        fs.mkdirs(new Path("/datascientist2"));

        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();

        String trainingFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/exposed/service/impl/train.dat");
        String testFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/exposed/service/impl/test.dat");
        String jsonFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/exposed/service/impl/iris.json");
        String decisionTreePythonScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/exposed/service/impl/dt_train.py");
        String logisticRegressionPythonScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/exposed/service/impl/lr_train.py");

        copyEntries.add(new CopyEntry(trainingFilePath, "/training", false));
        copyEntries.add(new CopyEntry(testFilePath, "/test", false));
        copyEntries.add(new CopyEntry(jsonFilePath, "/datascientist2", false));
        copyEntries.add(new CopyEntry(decisionTreePythonScriptPath, "/datascientist2", false));
        copyEntries.add(new CopyEntry(logisticRegressionPythonScriptPath, "/datascientist2", false));

        doCopy(fs, copyEntries);

        Algorithm decisionTreeAlgorithm = new Algorithm();
        decisionTreeAlgorithm.setName("DT");
        decisionTreeAlgorithm.setScript("/datascientist2/dt_train.py");
        decisionTreeAlgorithm.setPriority(1);
        decisionTreeAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=1");

        Algorithm logisticRegressionAlgorithm = new Algorithm();
        logisticRegressionAlgorithm.setName("LR");
        logisticRegressionAlgorithm.setScript("/datascientist2/lr_train.py");
        logisticRegressionAlgorithm.setPriority(0);
        logisticRegressionAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=0");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model1");
        modelDef.setAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { decisionTreeAlgorithm,
                logisticRegressionAlgorithm }));

        model = new Model();
        model.setModelDefinition(modelDef);
        model.setName("Model Submission1");
        model.setSchemaHdfsPath("/datascientist2/iris.json");
        model.setTrainingDataHdfsPath("/training/train.dat");
        model.setTestDataHdfsPath("/test/test.dat");
        model.setModelHdfsDir("/datascientist2/model");
        model.setFeatures(Arrays.<String> asList(new String[] { "sepal_length", "sepal_width", "petal_length",
                "petal_width" }));
        model.setTargets(Arrays.<String> asList(new String[] { "category" }));
        
    }

    @Test(groups = "functional")
    public void submitModel() throws Exception {
        List<ApplicationId> appIds = modelingService.submitModel(model);

        for (ApplicationId appId : appIds) {
            YarnApplicationState state = waitState(appId, 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
            assertNotNull(state);
            state = waitState(appId, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
            assertEquals(state, YarnApplicationState.FINISHED);

            NumberFormat appIdFormat = getAppIdFormat();
            String jobId = appId.getClusterTimestamp() + "_" + appIdFormat.format(appId.getId());
            String modelFile = HdfsHelper.getFilesForDir(yarnConfiguration, "/datascientist2/model/" + jobId).get(0);
            String modelContents = HdfsHelper.getHdfsFileContents(yarnConfiguration, modelFile);
            assertNotNull(modelContents);
        }
    }
    
    @Test(groups = "functional")
    public void throttleImmediate() throws Exception {
        model.setId(null);
        List<ApplicationId> appIds = modelingService.submitModel(model);
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setImmediate(true);
        config.setJobRankCutoff(2);
        modelingService.throttle(config);
        
        JobWatchdogService watchDog = getWatchdogService();
        watchDog.run(null);
        
        assertEquals(2, appIds.size());
        
        // First job to complete
        YarnApplicationState state = waitState(appIds.get(0), 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
        assertNotNull(state);
        state = waitState(appIds.get(0), 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
        assertEquals(state, YarnApplicationState.FINISHED);
        
        // Second job should have been killed since we throttled
        state = waitState(appIds.get(1), 10, TimeUnit.SECONDS, YarnApplicationState.KILLED);
    }
    
    @Test(groups = "functional"/*, dependsOnMethods = { "throttleImmediate "}*/)
    public void throttleNewlySubmittedModels() throws Exception {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setImmediate(false);
        config.setJobRankCutoff(2);
        modelingService.throttle(config);

        model.setId(null);
        List<ApplicationId> appIds = modelingService.submitModel(model);
        
        // Only one job would be submitted since new jobs won't even come in
        assertEquals(1, appIds.size());
        
        YarnApplicationState state = waitState(appIds.get(0), 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
        assertNotNull(state);
        state = waitState(appIds.get(0), 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
        assertEquals(state, YarnApplicationState.FINISHED);
        
    }

    private JobWatchdogService getWatchdogService() {
        JobWatchdogServiceImpl watchDog = new JobWatchdogServiceImpl();
        watchDog.setJobEntityMgr(jobEntityMgr);
        watchDog.setModelEntityMgr(modelEntityMgr);
        watchDog.setYarnService(yarnService);
        watchDog.setThrottleConfigurationEntityMgr(throttleConfigurationEntityMgr);
        watchDog.setJobService(jobService);
        return watchDog;
    }
    
    
}
