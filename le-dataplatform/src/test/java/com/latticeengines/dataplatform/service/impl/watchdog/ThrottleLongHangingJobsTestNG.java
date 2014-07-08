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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.client.yarn.AppMasterProperty;
import com.latticeengines.dataplatform.client.yarn.ContainerProperty;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.domain.exposed.dataplatform.Classifier;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;

@ContextConfiguration(locations = { "classpath:dataplatform-quartz-context.xml" })
public class ThrottleLongHangingJobsTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private ThrottleLongHangingJobs throttleLongHangingJobs;

    @Autowired
    private JobService jobService;

    private Classifier classifier1Min;
    private Classifier classifier2Mins;
    private Classifier classifier4Mins;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        System.out.println("default threshold value is: " + throttleLongHangingJobs.throttleThreshold);
        // Timeout set to 10s
        ReflectionTestUtils.setField(throttleLongHangingJobs, "throttleThreshold", 10000L);

        // Set up classifiers
        classifier1Min = setupClassifier("train_1min.py");
        classifier2Mins = setupClassifier("train_2mins.py");
        classifier4Mins = setupClassifier("train_4mins.py");

        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.delete(new Path("/training"), true);
        fs.delete(new Path("/test"), true);
        fs.delete(new Path("/scheduler"), true);

        fs.mkdirs(new Path("/training"));
        fs.mkdirs(new Path("/test"));
        fs.mkdirs(new Path("/scheduler"));

        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();

        String trainingFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_train.dat");
        String testFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_test.dat");
        String jsonFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/iris.json");
        String train1MinScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/fairscheduler/train_1min.py");
        String train2MinsScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/fairscheduler/train_2mins.py");
        String train4MinsScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/fairscheduler/train_4mins.py");

        copyEntries.add(new CopyEntry(trainingFilePath, "/training", false));
        copyEntries.add(new CopyEntry(testFilePath, "/test", false));
        copyEntries.add(new CopyEntry(jsonFilePath, "/scheduler", false));
        copyEntries.add(new CopyEntry(train1MinScriptPath, "/scheduler", false));
        copyEntries.add(new CopyEntry(train2MinsScriptPath, "/scheduler", false));
        copyEntries.add(new CopyEntry(train4MinsScriptPath, "/scheduler", false));

        doCopy(fs, copyEntries);
    }

    @Test(groups = "functional")
    public void testThrottleLongHangingJobs() throws Exception {
        ModelDefinition modelDef = produceModelDefinition();
        Model model = produceIrisMetadataModel();
        model.setModelDefinition(modelDef);

        List<ApplicationId> appIds = new ArrayList<ApplicationId>();

        for (int i = 0; i < 3; i++) {
            Job p0 = getJob(classifier1Min, "Priority0.0", 0, "DELL");
            model.addJob(p0);
            appIds.add(jobService.submitJob(p0));

            p0 = getJob(classifier2Mins, "Priority0.0", 0, "DELL");
            model.addJob(p0);
            appIds.add(jobService.submitJob(p0));

            p0 = getJob(classifier4Mins, "Priority0.0", 0, "DELL");
            model.addJob(p0);
            appIds.add(jobService.submitJob(p0));

            Thread.sleep(5000L);
        }

        waitForAllJobsToFinish(appIds);
    }

    private Classifier setupClassifier(String script) {
        Classifier classifier = new Classifier();
        classifier.setName("IrisClassifier");
        classifier.setFeatures(Arrays.<String> asList(new String[] { "sepal_length", "sepal_width", "petal_length",
                "petal_width" }));
        classifier.setTargets(Arrays.<String> asList(new String[] { "category" }));
        classifier.setSchemaHdfsPath("/scheduler/iris.json");
        classifier.setModelHdfsDir("/scheduler/result");
        classifier.setPythonScriptHdfsPath("/scheduler/" + script);
        classifier.setTrainingDataHdfsPath("/training/nn_train.dat");
        classifier.setTestDataHdfsPath("/test/nn_test.dat");
        classifier.setMetadataHdfsPath("/training/a.avsc");

        return classifier;
    }

    private Job getJob(Classifier classifier, String queue, int priority, String customer) {
        Job job = new Job();
        job.setClient("pythonClient");
        Properties[] properties = getPropertiesPair(classifier, queue, priority, customer);
        job.setAppMasterPropertiesObject(properties[0]);
        job.setContainerPropertiesObject(properties[1]);
        return job;
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
            JobStatus status = jobService.getJobStatus(appId.toString());
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
            jobStatus.put(appId, jobService.getJobReportById(appId));
        }
        return jobStatus;
    }

}
