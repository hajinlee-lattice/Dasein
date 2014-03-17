package com.latticeengines.dataplatform.fairscheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.entitymanager.impl.JobEntityMgrImpl;
import com.latticeengines.dataplatform.entitymanager.impl.ThrottleConfigurationEntityMgrImpl;
import com.latticeengines.dataplatform.exposed.domain.Classifier;
import com.latticeengines.dataplatform.exposed.domain.Job;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobService;

@ContextConfiguration(locations = { "classpath:dataplatform-quartz-context.xml" })
public class SchedulerTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private JobService jobService;
    
    @Autowired
    private JobEntityMgrImpl jobEntityMgr;

    @Autowired
    private ThrottleConfigurationEntityMgrImpl throttleConfigurationEntityMgr;
    

    private Classifier classifier1Min;
    private Classifier classifier2Mins;
    private Classifier classifier4Mins;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        jobEntityMgr.deleteStoreFile();
        throttleConfigurationEntityMgr.deleteStoreFile();
        classifier1Min = new Classifier();
        classifier1Min.setName("IrisClassifier");
        classifier1Min.setFeatures(Arrays.<String> asList(new String[] { "sepal_length", "sepal_width", "petal_length",
                "petal_width" }));
        classifier1Min.setTargets(Arrays.<String> asList(new String[] { "category" }));
        classifier1Min.setSchemaHdfsPath("/scheduler/iris.json");
        classifier1Min.setModelHdfsDir("/scheduler/result");
        classifier1Min.setPythonScriptHdfsPath("/scheduler/train_1min.py");
        classifier1Min.setTrainingDataHdfsPath("/training/train.dat");
        classifier1Min.setTestDataHdfsPath("/test/test.dat");

        classifier2Mins = new Classifier();
        classifier2Mins.setName("IrisClassifier");
        classifier2Mins.setFeatures(Arrays.<String> asList(new String[] { "sepal_length", "sepal_width",
                "petal_length", "petal_width" }));
        classifier2Mins.setTargets(Arrays.<String> asList(new String[] { "category" }));
        classifier2Mins.setSchemaHdfsPath("/scheduler/iris.json");
        classifier2Mins.setModelHdfsDir("/scheduler/result");
        classifier2Mins.setPythonScriptHdfsPath("/scheduler/train_2mins.py");
        classifier2Mins.setTrainingDataHdfsPath("/training/train.dat");
        classifier2Mins.setTestDataHdfsPath("/test/test.dat");

        classifier4Mins = new Classifier();
        classifier4Mins.setName("IrisClassifier");
        classifier4Mins.setFeatures(Arrays.<String> asList(new String[] { "sepal_length", "sepal_width",
                "petal_length", "petal_width" }));
        classifier4Mins.setTargets(Arrays.<String> asList(new String[] { "category" }));
        classifier4Mins.setSchemaHdfsPath("/scheduler/iris.json");
        classifier4Mins.setModelHdfsDir("/scheduler/result");
        classifier4Mins.setPythonScriptHdfsPath("/scheduler/train_4mins.py");
        classifier4Mins.setTrainingDataHdfsPath("/training/train.dat");
        classifier4Mins.setTestDataHdfsPath("/test/test.dat");

        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.delete(new Path("/training"), true);
        fs.delete(new Path("/test"), true);
        fs.delete(new Path("/scheduler"), true);

        fs.mkdirs(new Path("/training"));
        fs.mkdirs(new Path("/test"));
        fs.mkdirs(new Path("/scheduler"));

        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();

        String trainingFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/exposed/service/impl/train.dat");
        String testFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/exposed/service/impl/test.dat");
        String jsonFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/exposed/service/impl/iris.json");
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

    @Test(groups = "functional", enabled = false)
    public void testSubmit() throws Exception {
        List<ApplicationId> appIds = new ArrayList<ApplicationId>();
        // A
        for (int i = 0; i < 1; i++) {
            Job p0 = getJob(classifier1Min, "Priority0.A", 0);
            appIds.add(jobService.submitJob(p0));

            ///*
            for (int j = 0; j < 2; j++) {
                Job p1 = getJob(classifier2Mins, "Priority1.A", 1);
                appIds.add(jobService.submitJob(p1));
            }//*/

            Thread.sleep(5000L);
        }

        // B
        for (int i = 0; i < 1; i++) {
            Job p0 = getJob(classifier1Min, "Priority0.B", 0);
            appIds.add(jobService.submitJob(p0));

            ///*
            for (int j = 0; j < 2; j++) {
                Job p1 = getJob(classifier2Mins, "Priority1.B", 1);
                appIds.add(jobService.submitJob(p1));
            }//*/
            Thread.sleep(5000L);
        }

        
        // C
        for (int i = 0; i < 1; i++) {
            Job p0 = getJob(classifier1Min, "Priority0.C", 0);
            appIds.add(jobService.submitJob(p0));

            ///*
            for (int j = 0; j < 2; j++) {
                Job p1 = getJob(classifier2Mins, "Priority1.C", 1);
                appIds.add(jobService.submitJob(p1));
            }//*/
            Thread.sleep(5000L);
        }
        // D
        for (int i = 0; i < 1; i++) {
            Job p0 = getJob(classifier1Min, "Priority0.D", 0);
            appIds.add(jobService.submitJob(p0));

            ///*
            for (int j = 0; j < 2; j++) {
                Job p1 = getJob(classifier2Mins, "Priority1.D", 1);
                appIds.add(jobService.submitJob(p1));
            }//*/
            Thread.sleep(5000L);
        }
        // E
        for (int i = 0; i < 1; i++) {
            Job p0 = getJob(classifier1Min, "Priority0.E", 0);
            appIds.add(jobService.submitJob(p0));

            ///*
            for (int j = 0; j < 2; j++) {
                Job p1 = getJob(classifier2Mins, "Priority1.E", 1);
                appIds.add(jobService.submitJob(p1));
            }//*/
            Thread.sleep(5000L);
        }
        //waitForAllJobsToFinish(appIds);
        while (true);
    }

    @Test(groups = "functional", enabled = true)
    public void testSubmit2() throws Exception {
        List<ApplicationId> appIds = new ArrayList<ApplicationId>();
        // A
        for (int i = 0; i < 4; i++) {
            Job p0 = getJob(classifier1Min, "Priority0.A", 0);
            appIds.add(jobService.submitJob(p0));

            ///*
            for (int j = 0; j < 2; j++) {
                Job p1 = getJob(classifier2Mins, "Priority1.A", 1);
                appIds.add(jobService.submitJob(p1));
            }//*/

            Thread.sleep(5000L);
        }

        // B
        for (int i = 0; i < 1; i++) {
            Job p0 = getJob(classifier1Min, "Priority0.B", 0);
            appIds.add(jobService.submitJob(p0));

            ///*
            for (int j = 0; j < 2; j++) {
                Job p1 = getJob(classifier2Mins, "Priority1.B", 1);
                appIds.add(jobService.submitJob(p1));
            }//*/
            Thread.sleep(5000L);
        }

        
        // C
        for (int i = 0; i < 1; i++) {
            Job p0 = getJob(classifier1Min, "Priority0.C", 0);
            appIds.add(jobService.submitJob(p0));

            ///*
            for (int j = 0; j < 2; j++) {
                Job p1 = getJob(classifier2Mins, "Priority1.C", 1);
                appIds.add(jobService.submitJob(p1));
            }//*/
            //Thread.sleep(5000L);
        }
        //waitForAllJobsToFinish(appIds);
        while (true);
    }
    
    private Job getJob(Classifier classifier, String queue, int priority) {
        Job job = new Job();
        job.setClient("pythonClient");
        Properties[] properties = getPropertiesPair(classifier, queue, priority);
        job.setAppMasterProperties(properties[0]);
        job.setContainerProperties(properties[1]);
        return job;
    }

    private Properties[] getPropertiesPair(Classifier classifier, String queue, int priority) {
        Properties containerProperties = new Properties();
        containerProperties.put("VIRTUALCORES", "1");
        containerProperties.put("MEMORY", "1024");
        containerProperties.put("PRIORITY", Integer.toString(priority));
        containerProperties.put("METADATA", classifier.toString());

        Properties appMasterProperties = new Properties();
        appMasterProperties.put("QUEUE", queue);

        return new Properties[] { appMasterProperties, containerProperties };
    }

    private Map<ApplicationId, ApplicationReport> waitForAllJobsToFinish(List<ApplicationId> appIds) throws Exception {
        Map<ApplicationId, ApplicationReport> jobStatus = new HashMap<ApplicationId, ApplicationReport>();
        List<ApplicationId> jobStatusToCollect = new ArrayList<ApplicationId>(appIds);

        long startTime = System.currentTimeMillis();
        long nextReportTime = 60000L;
        while (!jobStatusToCollect.isEmpty()) {
            ApplicationId appId = jobStatusToCollect.get(0);
            YarnApplicationState state = waitState(appId, 30, TimeUnit.SECONDS, YarnApplicationState.FAILED,
                    YarnApplicationState.FINISHED);
            if (state == null) {
                System.out.println("ERROR: Invalid state detected");
                jobStatusToCollect.remove(appId);
                continue;
            }
            if (state.equals(YarnApplicationState.FAILED) || state.equals(YarnApplicationState.FINISHED)) {
                jobStatusToCollect.remove(appId);
                jobStatus.put(appId, jobService.getJobReportById(appId));
            }
            long runningTime = System.currentTimeMillis() - startTime;
            if (runningTime > nextReportTime) {
                nextReportTime += 60000L;
            }
        }
        return jobStatus;
    }
}
