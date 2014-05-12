package com.latticeengines.dataplatform.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.impl.JobEntityMgrImpl;
import com.latticeengines.dataplatform.entitymanager.impl.ModelEntityMgrImpl;
import com.latticeengines.dataplatform.entitymanager.impl.ThrottleConfigurationEntityMgrImpl;
import com.latticeengines.dataplatform.exposed.domain.Algorithm;
import com.latticeengines.dataplatform.exposed.domain.JobStatus;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ModelDefinition;
import com.latticeengines.dataplatform.exposed.domain.SamplingConfiguration;
import com.latticeengines.dataplatform.exposed.domain.SamplingElement;
import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;
import com.latticeengines.dataplatform.exposed.domain.algorithm.DecisionTreeAlgorithm;
import com.latticeengines.dataplatform.exposed.domain.algorithm.LogisticRegressionAlgorithm;
import com.latticeengines.dataplatform.exposed.domain.algorithm.RandomForestAlgorithm;
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
    
    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
        throttleConfigurationEntityMgr.deleteStoreFile();
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        modelEntityMgr.deleteStoreFile();
        
        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.delete(new Path("/user/s-analytics/customers/DELL"), true);

        fs.mkdirs(new Path("/user/s-analytics/customers/DELL/data/DELL_EVENT_TABLE_TEST"));

        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();

        String inputDir = ClassLoader.getSystemResource("com/latticeengines/dataplatform/exposed/service/impl/DELL_EVENT_TABLE_TEST").getPath();
        File[] avroFiles = getAvroFilesForDir(inputDir);
        for (File avroFile : avroFiles) {
            copyEntries.add(new CopyEntry("file:" + avroFile.getAbsolutePath(), "/user/s-analytics/customers/DELL/data/DELL_EVENT_TABLE_TEST", false));
        }

        doCopy(fs, copyEntries);

        LogisticRegressionAlgorithm logisticRegressionAlgorithm = new LogisticRegressionAlgorithm();
        logisticRegressionAlgorithm.setPriority(0);
        logisticRegressionAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=0");
        logisticRegressionAlgorithm.setSampleName("s0");

        DecisionTreeAlgorithm decisionTreeAlgorithm = new DecisionTreeAlgorithm();
        decisionTreeAlgorithm.setPriority(1);
        decisionTreeAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=1");
        decisionTreeAlgorithm.setSampleName("s1");

        RandomForestAlgorithm randomForestAlgorithm = new RandomForestAlgorithm();
        randomForestAlgorithm.setPriority(2);
        randomForestAlgorithm.setContainerProperties("VIRTUALCORES=1 MEMORY=64 PRIORITY=1");
        randomForestAlgorithm.setSampleName("all");
        randomForestAlgorithm.setAlgorithmProperties("criterion=gini n_estimators=3");

        ModelDefinition modelDef = new ModelDefinition();
        modelDef.setName("Model1");
        modelDef.setAlgorithms(Arrays.<Algorithm> asList(new Algorithm[] { decisionTreeAlgorithm, randomForestAlgorithm,
                logisticRegressionAlgorithm }));

        model = createModel(modelDef);
    }
    
    private Model createModel(ModelDefinition modelDef) {
        Model m = new Model();
        m.setModelDefinition(modelDef);
        m.setName("Model Submission1");

        m.setTable("DELL_EVENT_TABLE_TEST");
        m.setFeatures(Arrays.<String> asList(new String[] {
                "Column5", //
                "Column6", //
                "Column7", //
                "Column8", //
                "Column9", //
                "Column10" }));
        m.setTargets(Arrays.<String> asList(new String[] { "Event_Latitude_Customer" }));
        m.setKeyCols(Arrays.<String> asList(new String[] { "IDX" }));
        m.setCustomer("DELL");
        m.setDataFormat("avro");
        
        return m;
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
        ApplicationId appId = modelingService.createSamples(samplingConfig);
        YarnApplicationState state = waitState(appId, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
        assertEquals(state, YarnApplicationState.FINISHED);
    }
    
    @Test(groups = "functional", enabled = true, dependsOnMethods = { "createSamples" })
    public void submitModel() throws Exception {
        List<ApplicationId> appIds = modelingService.submitModel(model);

        for (ApplicationId appId : appIds) {
            YarnApplicationState state = waitState(appId, 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
            assertNotNull(state);
            state = waitState(appId, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
            assertEquals(state, YarnApplicationState.FINISHED);

            JobStatus jobStatus = modelingService.getJobStatus(appId.toString());
            String modelFile = HdfsHelper.getFilesForDir(yarnConfiguration, jobStatus.getResultDirectory()).get(0);
            String modelContents = HdfsHelper.getHdfsFileContents(yarnConfiguration, modelFile);
            assertNotNull(modelContents);
        }
    }
    
    @Test(groups = "functional", enabled = true, dependsOnMethods = { "submitModel" })
    public void submitModelMultithreaded() throws Exception {
        
        ExecutorService executor = Executors.newFixedThreadPool(3);
        
        final Model[] models = new Model[3];
        models[0] = createModel(model.getModelDefinition());
        models[1] = createModel(model.getModelDefinition());
        models[2] = createModel(model.getModelDefinition());
        
        List<Future<List<ApplicationId>>> futures = new ArrayList<Future<List<ApplicationId>>>();
        for (int i = 0; i < 3; i++) {
            final Model m = models[i];
            futures.add(executor.submit(new Callable<List<ApplicationId>>() {

                @Override
                public List<ApplicationId> call() throws Exception {
                    return modelingService.submitModel(m);
                }
                
            }));
        }
        List<ApplicationId> appIds = new ArrayList<ApplicationId>();
        
        for (Future<List<ApplicationId>> future : futures) {
            appIds.addAll(future.get());
        }

        for (ApplicationId appId : appIds) {
            YarnApplicationState state = waitState(appId, 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
            assertNotNull(state);
            state = waitState(appId, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
            assertEquals(state, YarnApplicationState.FINISHED);

            JobStatus jobStatus = modelingService.getJobStatus(appId.toString());
            String modelFile = HdfsHelper.getFilesForDir(yarnConfiguration, jobStatus.getResultDirectory()).get(0);
            String modelContents = HdfsHelper.getHdfsFileContents(yarnConfiguration, modelFile);
            assertNotNull(modelContents);
        }
    }
    
    @Test(groups = "functional", dependsOnMethods = { "submitModel" })
    public void throttleImmediate() throws Exception {
        model.setId(null);
        List<ApplicationId> appIds = modelingService.submitModel(model);
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setImmediate(true);
        config.setJobRankCutoff(2);
        modelingService.throttle(config);
        
        JobWatchdogService watchDog = getWatchdogService();
        watchDog.run(null);
        
        assertEquals(3, appIds.size());
        
        // First job to complete
        YarnApplicationState state = waitState(appIds.get(0), 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
        assertNotNull(state);
        state = waitState(appIds.get(0), 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
        assertEquals(state, YarnApplicationState.FINISHED);
        
        // Second job should have been killed since we throttled
        state = waitState(appIds.get(1), 10, TimeUnit.SECONDS, YarnApplicationState.KILLED);
    }
    
    @Test(groups = "functional", dependsOnMethods = { "throttleImmediate" })
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
