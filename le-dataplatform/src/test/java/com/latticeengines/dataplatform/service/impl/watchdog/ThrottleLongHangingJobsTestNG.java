package com.latticeengines.dataplatform.service.impl.watchdog;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelDefinition;
import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class ThrottleLongHangingJobsTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private ThrottleLongHangingJobs throttleLongHangingJobs;

    @Autowired
    private YarnService yarnService;

    @Autowired
    private ModelingJobService modelingJobService;
    
    @Autowired
    private VersionManager versionManager;

    @Autowired
    private JobEntityMgr jobEntityMgr;

    private Classifier classifier1Min;
    private Classifier classifier2Mins;
    private Classifier classifier4Mins;

    private String baseDir = "/functionalTests/" + suffix;

    @BeforeClass(groups = {"functional"})
    public void setup() throws Exception {
        // Set up classifiers
        classifier1Min = setupClassifier("train_1min.py");
        classifier2Mins = setupClassifier("train_2mins.py");
        classifier4Mins = setupClassifier("train_4mins.py");

        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.mkdirs(new Path(baseDir + "/training"));
        fs.mkdirs(new Path(baseDir + "/test"));
        fs.mkdirs(new Path(baseDir + "/scheduler"));

        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();

        String trainingFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_train.dat");
        String testFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_test.dat");
        String jsonFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/iris.json");
        String train1MinScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/fairscheduler/train_1min.py");
        String train2MinsScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/fairscheduler/train_2mins.py");
        String train4MinsScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/fairscheduler/train_4mins.py");

        copyEntries.add(new CopyEntry(trainingFilePath, baseDir + "/training", false));
        copyEntries.add(new CopyEntry(testFilePath, baseDir + "/test", false));
        copyEntries.add(new CopyEntry(jsonFilePath, baseDir + "/scheduler", false));
        copyEntries.add(new CopyEntry(train1MinScriptPath, baseDir + "/scheduler", false));
        copyEntries.add(new CopyEntry(train2MinsScriptPath, baseDir + "/scheduler", false));
        copyEntries.add(new CopyEntry(train4MinsScriptPath, baseDir + "/scheduler", false));

        doCopy(fs, copyEntries);

        // Timeout set to 10s
        ReflectionTestUtils.setField(throttleLongHangingJobs, "throttleThreshold", 10000L);
        throttleLongHangingJobs.setModelingJobService(modelingJobService);
        throttleLongHangingJobs.setYarnService(yarnService);
        throttleLongHangingJobs.setJobEntityMgr(jobEntityMgr);
        // Runs every 15 seconds
        this.startQuartzJob(new Runnable() {

            @Override
            public void run() {
                try {
                    throttleLongHangingJobs.run(null);
                } catch (JobExecutionException e) {
                    e.printStackTrace();
                }
            }
        }, 15L);
    }

    @AfterClass(groups = {"functional"})
    public void tearDown() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path("/functionalTests"), true);
    }

    @Test(groups = {"functional"})
    public void testThrottleLongHangingJobs() throws Exception {
        ModelDefinition modelDef = produceModelDefinition();
        Model model = produceIrisMetadataModel();
        model.setModelDefinition(modelDef);

        List<ApplicationId> appIds = new ArrayList<ApplicationId>();

        for (int i = 0; i < 3; i++) {
            ModelingJob p0 = getJob(classifier1Min, 0, "DELL");
            model.addModelingJob(p0);
            appIds.add(modelingJobService.submitJob(p0));

            p0 = getJob(classifier2Mins, 0, "DELL");
            model.addModelingJob(p0);
            appIds.add(modelingJobService.submitJob(p0));

            p0 = getJob(classifier4Mins, 0, "DELL");
            model.addModelingJob(p0);
            appIds.add(modelingJobService.submitJob(p0));

            Thread.sleep(5000L);
        }

        waitForAllJobsToFinish(appIds);
        this.stopQuartzJob();
    }

    private Classifier setupClassifier(String script) {
        Classifier classifier = new Classifier();
        classifier.setName("IrisClassifier");
        classifier.setFeatures(Arrays.<String> asList(new String[] { "sepal_length", "sepal_width", "petal_length",
                "petal_width" }));
        classifier.setTargets(Arrays.<String> asList(new String[] { "category" }));
        classifier.setSchemaHdfsPath(baseDir + "/scheduler/iris.json");
        classifier.setModelHdfsDir(baseDir + "/scheduler/result");
        classifier.setPythonScriptHdfsPath(baseDir + "/scheduler/" + script);
        classifier.setTrainingDataHdfsPath(baseDir + "/training/nn_train.dat");
        classifier.setTestDataHdfsPath(baseDir + "/test/nn_test.dat");
        classifier.setDataProfileHdfsPath(baseDir + "/training/a.avro");
        classifier.setConfigMetadataHdfsPath(baseDir + "/training/a.avsc");
        classifier.setPythonPipelineLibHdfsPath("/app/" + versionManager.getCurrentVersion() + "/dataplatform/scripts/lepipeline.tar.gz");
        classifier.setPythonPipelineScriptHdfsPath("/app/" + versionManager.getCurrentVersion() + "/dataplatform/scripts/pipeline.py");

        return classifier;
    }

    private ModelingJob getJob(Classifier classifier, int priority, String customer) {
        ModelingJob modelingJob = new ModelingJob();
        modelingJob.setClient("pythonClient");
        Properties[] properties = getPropertiesPair(classifier, LedpQueueAssigner.getModelingQueueNameForSubmission(), priority, customer);
        modelingJob.setAppMasterPropertiesObject(properties[0]);
        modelingJob.setContainerPropertiesObject(properties[1]);
        return modelingJob;
    }

    private Properties[] getPropertiesPair(Classifier classifier, String queue, int priority, String customer) {
        Properties containerProperties = new Properties();
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "1024");
        containerProperties.put(ContainerProperty.PRIORITY.name(), Integer.toString(priority));
        containerProperties.put(ContainerProperty.METADATA.name(), classifier.toString());

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), queue);
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), customer);

        return new Properties[] { appMasterProperties, containerProperties };
    }

    private Map<ApplicationId, ApplicationReport> waitForAllJobsToFinish(List<ApplicationId> appIds) throws Exception {
        Map<ApplicationId, ApplicationReport> jobStatus = new HashMap<ApplicationId, ApplicationReport>();
        List<ApplicationId> jobStatusToCollect = new ArrayList<ApplicationId>(appIds);

        while (!jobStatusToCollect.isEmpty()) {
            ApplicationId appId = jobStatusToCollect.get(0);
            JobStatus status = modelingJobService.getJobStatus(appId.toString());
            FinalApplicationStatus appStatus = waitForStatus(getApplicationId(status.getId()),
                    FinalApplicationStatus.KILLED);
            System.out.println("===============================ThrottleLongHangingJobTestNG.waitForAllJobsToFinish()");
            if (appStatus == null) {
                System.out.println("ERROR: Invalid state detected");
                jobStatusToCollect.remove(appId);
                continue;
            }
            // All jobs should be throttled
            assertEquals(appStatus, FinalApplicationStatus.KILLED);
            jobStatusToCollect.remove(appId);
            jobStatus.put(appId, modelingJobService.getJobReportById(appId));
        }
        return jobStatus;
    }

}
