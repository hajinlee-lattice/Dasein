package com.latticeengines.dataplatform.fairscheduler;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
import org.springframework.yarn.client.YarnClient;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.dataplatform.client.yarn.AppMasterProperty;
import com.latticeengines.dataplatform.client.yarn.ContainerProperty;
import com.latticeengines.dataplatform.entitymanager.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.domain.exposed.dataplatform.Classifier;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ModelDefinition;

/**
 * This test working is dependent on the cluster settings. Ensure that:
 *
 * 1. Set in yarn-site.xml <property>
 * <name>yarn.nodemanager.resource.cpu-vcores</name> <value>#cores in
 * system</value> </property>
 *
 * 2. Set in fair-scheduler.xml <user name="User running resource manager">
 * <maxRunningApps>(#cores in system)/2</maxRunningApps> </user>
 *
 * @author rgonzalez
 *
 */
@ContextConfiguration(locations = { "classpath:dataplatform-quartz-context.xml" })
public class SchedulerTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private JobService jobService;

    @Autowired
    private YarnClient defaultYarnClient;

    @Autowired
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;

    private Classifier classifier1Min;
    private Classifier classifier2Mins;
    private Classifier classifier4Mins;

    @BeforeClass(groups = "functional.scheduler")
    public void setup() throws Exception {
        // TODO remove this once we rollback all test db changes
        throttleConfigurationEntityMgr.cleanUpAllConfiguration();

        classifier1Min = new Classifier();
        classifier1Min.setName("IrisClassifier");
        classifier1Min.setFeatures(Arrays.<String> asList(new String[] { "sepal_length", "sepal_width", "petal_length",
                "petal_width" }));
        classifier1Min.setTargets(Arrays.<String> asList(new String[] { "category" }));
        classifier1Min.setSchemaHdfsPath("/scheduler/iris.json");
        classifier1Min.setModelHdfsDir("/scheduler/result");
        classifier1Min.setPythonScriptHdfsPath("/scheduler/train_1min.py");
        classifier1Min.setTrainingDataHdfsPath("/training/nn_train.dat");
        classifier1Min.setTestDataHdfsPath("/test/nn_test.dat");
        classifier1Min.setDataProfileHdfsPath("/training/a.avro");
        classifier1Min.setConfigMetadataHdfsPath("/training/a.avsc");

        classifier2Mins = new Classifier();
        classifier2Mins.setName("IrisClassifier");
        classifier2Mins.setFeatures(Arrays.<String> asList(new String[] { "sepal_length", "sepal_width",
                "petal_length", "petal_width" }));
        classifier2Mins.setTargets(Arrays.<String> asList(new String[] { "category" }));
        classifier2Mins.setSchemaHdfsPath("/scheduler/iris.json");
        classifier2Mins.setModelHdfsDir("/scheduler/result");
        classifier2Mins.setPythonScriptHdfsPath("/scheduler/train_2mins.py");
        classifier2Mins.setTrainingDataHdfsPath("/training/nn_train.dat");
        classifier2Mins.setTestDataHdfsPath("/test/nn_test.dat");
        classifier2Mins.setDataProfileHdfsPath("/training/a.avro");
        classifier2Mins.setConfigMetadataHdfsPath("/training/a.avsc");

        classifier4Mins = new Classifier();
        classifier4Mins.setName("IrisClassifier");
        classifier4Mins.setFeatures(Arrays.<String> asList(new String[] { "sepal_length", "sepal_width",
                "petal_length", "petal_width" }));
        classifier4Mins.setTargets(Arrays.<String> asList(new String[] { "category" }));
        classifier4Mins.setSchemaHdfsPath("/scheduler/iris.json");
        classifier4Mins.setModelHdfsDir("/scheduler/result");
        classifier4Mins.setPythonScriptHdfsPath("/scheduler/train_4mins.py");
        classifier4Mins.setTrainingDataHdfsPath("/training/nn_train.dat");
        classifier4Mins.setTestDataHdfsPath("/test/nn_test.dat");
        classifier4Mins.setDataProfileHdfsPath("/training/a.avro");
        classifier4Mins.setConfigMetadataHdfsPath("/training/a.avsc");

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

    @Test(groups = "functional.scheduler", enabled = true)
    public void testSubmit() throws Exception {
        ModelDefinition modelDef = produceModelDefinition();
        Model model = produceIrisMetadataModel();
        model.setModelDefinition(modelDef);

        List<ApplicationId> appIds = new ArrayList<ApplicationId>();
        // A
        for (int i = 0; i < 1; i++) {
            Job p0 = createJob(classifier1Min, "Priority0.0", 0, "DELL");
            model.addJob(p0);
            appIds.add(jobService.submitJob(p0));

            // /*
            for (int j = 0; j < 2; j++) {
                Job p1 = createJob(classifier2Mins, "Priority1.0", 1, "DELL");
                model.addJob(p1);
                appIds.add(jobService.submitJob(p1));
            }
            // */

            Thread.sleep(5000L);
        }

        // B
        for (int i = 0; i < 1; i++) {
            Job p0 = createJob(classifier1Min, "Priority0.1", 0, "DELL");
            model.addJob(p0);
            appIds.add(jobService.submitJob(p0));

            // /*
            for (int j = 0; j < 2; j++) {
                Job p1 = createJob(classifier2Mins, "Priority1.1", 1, "DELL");
                model.addJob(p1);
                appIds.add(jobService.submitJob(p1));
            }// */
            Thread.sleep(5000L);
        }

        // C
        for (int i = 0; i < 1; i++) {
            Job p0 = createJob(classifier1Min, "Priority0.2", 0, "DELL");
            model.addJob(p0);
            appIds.add(jobService.submitJob(p0));

            // /*
            for (int j = 0; j < 2; j++) {
                Job p1 = createJob(classifier2Mins, "Priority1.2", 1, "DELL");
                model.addJob(p1);
                appIds.add(jobService.submitJob(p1));
            }// */
            Thread.sleep(5000L);
        }
        // D
        for (int i = 0; i < 1; i++) {
            Job p0 = createJob(classifier1Min, "Priority0.3", 0, "DELL");
            model.addJob(p0);
            appIds.add(jobService.submitJob(p0));

            // /*
            for (int j = 0; j < 2; j++) {
                Job p1 = createJob(classifier2Mins, "Priority1.3", 1, "DELL");
                model.addJob(p1);
                appIds.add(jobService.submitJob(p1));
            }// */
            Thread.sleep(5000L);
        }
        // E
        for (int i = 0; i < 1; i++) {
            Job p0 = createJob(classifier1Min, "Priority0.4", 0, "DELL");
            model.addJob(p0);
            appIds.add(jobService.submitJob(p0));

            // /*
            for (int j = 0; j < 2; j++) {
                Job p1 = createJob(classifier2Mins, "Priority1.4", 1, "DELL");
                model.addJob(p1);
                appIds.add(jobService.submitJob(p1));
            }// */
            Thread.sleep(5000L);
        }

        waitForAllJobsToFinishThenConfirmAllSucceeded(appIds);
//        assertTrue(countPremptedJobs(appIds) > 0);
    }

    @Test(groups = "functional.scheduler", enabled = true)
    public void testSubmit2() throws Exception {
        ModelDefinition modelDef = produceModelDefinition();
        Model model = produceIrisMetadataModel();
        model.setModelDefinition(modelDef);

        List<ApplicationId> appIds = new ArrayList<ApplicationId>();
        // A
        for (int i = 0; i < 4; i++) {
            Job p0 = createJob(classifier1Min, "Priority0.0", 0, "DELL");
            model.addJob(p0);
            appIds.add(jobService.submitJob(p0));

            // /*
            for (int j = 0; j < 2; j++) {
                Job p1 = createJob(classifier2Mins, "Priority1.0", 1, "DELL");
                model.addJob(p1);
                appIds.add(jobService.submitJob(p1));
            }// */

            Thread.sleep(5000L);
        }

        // B
        for (int i = 0; i < 1; i++) {
            Job p0 = createJob(classifier1Min, "Priority0.1", 0, "DELL");
            model.addJob(p0);
            appIds.add(jobService.submitJob(p0));

            // /*
            for (int j = 0; j < 2; j++) {
                Job p1 = createJob(classifier2Mins, "Priority1.1", 1, "DELL");
                model.addJob(p1);
                appIds.add(jobService.submitJob(p1));
            }// */
            Thread.sleep(5000L);
        }

        // C
        for (int i = 0; i < 1; i++) {
            Job p0 = createJob(classifier1Min, "Priority0.2", 0, "DELL");
            model.addJob(p0);
            appIds.add(jobService.submitJob(p0));

            // /*
            for (int j = 0; j < 2; j++) {
                Job p1 = createJob(classifier2Mins, "Priority1.2", 1, "DELL");
                model.addJob(p1);
                appIds.add(jobService.submitJob(p1));
            }// */
            Thread.sleep(5000L);
        }
        waitForAllJobsToFinishThenConfirmAllSucceeded(appIds);
//        assertTrue(countPremptedJobs(appIds) > 0);
    }

    private Job createJob(Classifier classifier, String queue, int priority, String customer) {
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

    private Map<ApplicationId, ApplicationReport> waitForAllJobsToFinishThenConfirmAllSucceeded(List<ApplicationId> appIds) throws Exception {
        Map<ApplicationId, ApplicationReport> jobStatus = new HashMap<ApplicationId, ApplicationReport>();
        List<ApplicationId> jobStatusToCollect = new ArrayList<ApplicationId>(appIds);
        int successCount = 0;
        while (!jobStatusToCollect.isEmpty()) {
            ApplicationId appId = jobStatusToCollect.get(0);
            JobStatus status = jobService.getJobStatus(appId.toString());
            FinalApplicationStatus appStatus = waitForStatus(getApplicationId(status.getId()), FinalApplicationStatus.SUCCEEDED);

            if (appStatus == null) {
                System.out.println("ERROR: Invalid state detected");
                jobStatusToCollect.remove(appId);
                continue;
            }
            if (appStatus == FinalApplicationStatus.SUCCEEDED) {
                successCount++;
            }
            if (TERMINAL_STATUS.contains(appStatus)) {
                jobStatusToCollect.remove(appId);
                jobStatus.put(appId, jobService.getJobReportById(appId));
            }
        }

        assertEquals(successCount, appIds.size());

        return jobStatus;
    }

    private int countPremptedJobs(List<ApplicationId> appIds) {
        int total = 0;

        for (ApplicationId applicationId : appIds) {
            ApplicationReport appReport = defaultYarnClient.getApplicationReport(applicationId);
            if (YarnUtils.isPrempted(appReport.getDiagnostics())) {
                total++;
            }
        }

        return total;
    }


}
