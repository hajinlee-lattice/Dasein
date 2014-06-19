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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.JobWatchdogService;
import com.latticeengines.dataplatform.service.impl.JobWatchdogServiceImpl;
import com.latticeengines.domain.exposed.dataplatform.DataProfileConfiguration;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.domain.exposed.dataplatform.SamplingElement;
import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

@Transactional 
public class ModelingServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private JobService jobService;

    @Autowired
    private YarnService yarnService;

    @Autowired
    private ModelingService modelingService;

    private Model model = null;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {

    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path("/user/s-analytics/customers/DELL"), true);
        fs.mkdirs(new Path("/user/s-analytics/customers/DELL/data/DELL_EVENT_TABLE_TEST"));
        fs.mkdirs(new Path("/user/s-analytics/customers/DELL/data/DELL_EVENT_TABLE_TEST/EventMetadata"));

        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();

        String inputDir = ClassLoader.getSystemResource(
                "com/latticeengines/dataplatform/exposed/service/impl/DELL_EVENT_TABLE_TEST").getPath();
        File[] avroFiles = getAvroFilesForDir(inputDir);
        for (File avroFile : avroFiles) {
            copyEntries.add(new CopyEntry("file:" + avroFile.getAbsolutePath(),
                    "/user/s-analytics/customers/DELL/data/DELL_EVENT_TABLE_TEST", false));
        }

        doCopy(fs, copyEntries);

        ModelDefinition modelDef = produceModelDefinition();
        // 
        // in the application, it is assumed that the model definition is defined in the metadata db
        // also, modelDef 'name' should be unique
        modelDefinitionEntityMgr.createOrUpdate(modelDef);
        //
        model = produceModel(modelDef);
    }

    private Model produceModel(ModelDefinition modelDef) {
        Model m = new Model();
        m.setModelDefinition(modelDef);
        m.setName("Model Submission-"+System.currentTimeMillis());
        m.setTable("DELL_EVENT_TABLE_TEST");
        m.setMetadataTable("EventMetadata");
        m.setFeaturesList(Arrays.<String> asList(new String[] {
                "Column5", //
                "Column6", //
                "Column7", //
                "Column8", //
                "Column9", //
                "Column10" }));
        m.setTargetsList(Arrays.<String> asList(new String[] { "Event_Latitude_Customer" }));
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
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Test(groups = "functional", enabled = true, dependsOnMethods = { "createSamples" })
    public void profileData() throws Exception {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setCustomer(model.getCustomer());
        config.setTable(model.getTable());
        config.setMetadataTable(model.getMetadataTable());
        config.setSamplePrefix("all");
        List<String> excludeList = new ArrayList<>();
        excludeList.add("IDX");
        excludeList.add("CustomerID");
        excludeList.add("LeadID");
        excludeList.add("Target_LatitudeOptiplex_Retention_PCA_PPA_Customer");
        config.setExcludeColumnList(excludeList);
        ApplicationId appId = modelingService.profileData(config);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "functional", enabled = true, dependsOnMethods = { "profileData" })
    public void submitModel() throws Exception {
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
 
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Test(groups = "functional", enabled = true, dependsOnMethods = { "submitModel" })
    public void submitModelMultithreaded() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        
        final Model[] models = new Model[3];
        ModelDefinition modelDef = produceModelDefinition(); 
        modelDefinitionEntityMgr.createOrUpdate(modelDef);
        models[0] = produceModel(modelDef);
        modelDefinitionEntityMgr.createOrUpdate(modelDef);
        models[1] = produceModel(modelDef);
        modelDefinitionEntityMgr.createOrUpdate(modelDef);
        models[2] = produceModel(modelDef);

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
            FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
            assertEquals(status, FinalApplicationStatus.SUCCEEDED);

            JobStatus jobStatus = modelingService.getJobStatus(appId.toString());
            String modelFile = HdfsUtils.getFilesForDir(yarnConfiguration, jobStatus.getResultDirectory()).get(0);
            String modelContents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelFile);
            assertNotNull(modelContents);
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "submitModel" })
    @Transactional(propagation = Propagation.REQUIRED)
    public void throttleImmediate() throws Exception {
        // clean up:  this test case expects no previous throttle
        throttleConfigurationEntityMgr.deleteAll();
        
        ModelDefinition modelDef = produceModelDefinition();
        Model m = produceModel(modelDef);
        m.setModelDefinition(modelDef);
        List<ApplicationId> appIds = modelingService.submitModel(m);
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setImmediate(true);
        config.setJobRankCutoff(2);
        // save the throttle configuration
        modelingService.throttle(config);

        JobWatchdogService watchDog = getWatchdogService();
        watchDog.run(null);

        assertEquals(appIds.size(), 3);

        // First job to complete  
        FinalApplicationStatus status = waitForStatus(appIds.get(0), FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        // Second job should have been killed since we throttled
        status = waitForStatus(appIds.get(1), FinalApplicationStatus.KILLED);
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Test(groups = "functional", dependsOnMethods = { "throttleImmediate" })
    public void throttleNewlySubmittedModels() throws Exception {
        ThrottleConfiguration config = new ThrottleConfiguration();
        config.setImmediate(false);
        config.setJobRankCutoff(2);
        modelingService.throttle(config);

        ModelDefinition modelDef = produceModelDefinition();
        Model m = produceModel(modelDef);
        m.setModelDefinition(modelDef);
        List<ApplicationId> appIds = modelingService.submitModel(m);

        // Only one job would be submitted since new jobs won't even come in
        assertEquals(appIds.size(), 1);
        
        FinalApplicationStatus status = waitForStatus(appIds.get(0), FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
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
