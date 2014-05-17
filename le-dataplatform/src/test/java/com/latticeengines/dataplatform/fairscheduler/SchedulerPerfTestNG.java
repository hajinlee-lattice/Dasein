package com.latticeengines.dataplatform.fairscheduler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.client.yarn.AppMasterProperty;
import com.latticeengines.dataplatform.client.yarn.ContainerProperty;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.domain.exposed.dataplatform.Classifier;
import com.latticeengines.perf.exposed.test.PerfFunctionalTestBase;

/**
 * <?xml version="1.0"?> <allocations> <queue name="Priority0">
 * <weight>1000</weight> <queue name="A"> <minResources>1100 mb,2
 * vcores</minResources> <schedulingPolicy>fifo</schedulingPolicy> </queue>
 * <queue name="B"> <minResources>1100 mb,2 vcores</minResources>
 * <schedulingPolicy>fifo</schedulingPolicy> </queue> <queue name="C">
 * <minResources>1100 mb,2 vcores</minResources>
 * <schedulingPolicy>fifo</schedulingPolicy> </queue> </queue> <queue
 * name="Priority1"> <weight>10</weight> <queue name="A">
 * <schedulingPolicy>fifo</schedulingPolicy> </queue> <queue name="B">
 * <schedulingPolicy>fifo</schedulingPolicy> </queue> <queue name="C">
 * <schedulingPolicy>fifo</schedulingPolicy> </queue>
 * 
 * </queue> <queue name="Priority2"> <weight>1</weight> <queue name="A">
 * <schedulingPolicy>fifo</schedulingPolicy> </queue> <queue name="B">
 * <schedulingPolicy>fifo</schedulingPolicy> </queue> <queue name="C">
 * <schedulingPolicy>fifo</schedulingPolicy> </queue>
 * 
 * </queue>
 * <defaultMinSharePreemptionTimeout>30</defaultMinSharePreemptionTimeout>
 * <fairSharePreemptionTimeout>30</fairSharePreemptionTimeout> </allocations>
 */
public class SchedulerPerfTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private JobService jobService;
    
    @Autowired
    private Configuration yarnConfiguration;

    private Classifier classifier1Min;
    private Classifier classifier5Mins;
    private Classifier classifier10Mins;
    private Map<String, List<List<List<ApplicationId>>>> customerJobsToAppIdMap = new HashMap<String, List<List<List<ApplicationId>>>>();

    private PerfFunctionalTestBase perfTestBase;

    @Override
    protected boolean doYarnClusterSetup() {
        return true;
    }

    @Override
    protected boolean doDependencyLibraryCopy() {
        return false;
    }

    @BeforeClass(groups = "perf")
    public void setup() throws Exception {
        new File("/tmp/metricfile.txt").delete();
        perfTestBase = new PerfFunctionalTestBase("/tmp/metricfile.txt");
        perfTestBase.beforeClass();

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

        classifier5Mins = new Classifier();
        classifier5Mins.setName("IrisClassifier");
        classifier5Mins.setFeatures(Arrays.<String> asList(new String[] { "sepal_length", "sepal_width",
                "petal_length", "petal_width" }));
        classifier5Mins.setTargets(Arrays.<String> asList(new String[] { "category" }));
        classifier5Mins.setSchemaHdfsPath("/scheduler/iris.json");
        classifier5Mins.setModelHdfsDir("/scheduler/result");
        classifier5Mins.setPythonScriptHdfsPath("/scheduler/train_5mins.py");
        classifier5Mins.setTrainingDataHdfsPath("/training/train.dat");
        classifier5Mins.setTestDataHdfsPath("/test/test.dat");

        classifier10Mins = new Classifier();
        classifier10Mins.setName("IrisClassifier");
        classifier10Mins.setFeatures(Arrays.<String> asList(new String[] { "sepal_length", "sepal_width",
                "petal_length", "petal_width" }));
        classifier10Mins.setTargets(Arrays.<String> asList(new String[] { "category" }));
        classifier10Mins.setSchemaHdfsPath("/scheduler/iris.json");
        classifier10Mins.setModelHdfsDir("/scheduler/result");
        classifier10Mins.setPythonScriptHdfsPath("/scheduler/train_10mins.py");
        classifier10Mins.setTrainingDataHdfsPath("/training/train.dat");
        classifier10Mins.setTestDataHdfsPath("/test/test.dat");

        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.delete(new Path("/training"), true);
        fs.delete(new Path("/test"), true);
        fs.delete(new Path("/scheduler"), true);

        fs.mkdirs(new Path("/training"));
        fs.mkdirs(new Path("/test"));
        fs.mkdirs(new Path("/scheduler"));

        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();

        String trainingFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_train.dat");
        String testFilePath = getFileUrlFromResource("com/latticeengines/dataplatform//service/impl/nn_test.dat");
        String jsonFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/iris.json");
        String train1MinScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/fairscheduler/train_1min.py");
        String train5MinsScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/fairscheduler/train_5mins.py");
        String train10MinsScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/fairscheduler/train_10mins.py");

        copyEntries.add(new CopyEntry(trainingFilePath, "/training", false));
        copyEntries.add(new CopyEntry(testFilePath, "/test", false));
        copyEntries.add(new CopyEntry(jsonFilePath, "/scheduler", false));
        copyEntries.add(new CopyEntry(train1MinScriptPath, "/scheduler", false));
        copyEntries.add(new CopyEntry(train5MinsScriptPath, "/scheduler", false));
        copyEntries.add(new CopyEntry(train10MinsScriptPath, "/scheduler", false));

        doCopy(fs, copyEntries);
    }

    @BeforeMethod(groups = "perf")
    public void beforeMethod() {
        perfTestBase.beforeMethod();
    }

    @AfterMethod(groups = "perf")
    public void afterMethod() {
        customerJobsToAppIdMap.clear();
        perfTestBase.afterMethod();
        perfTestBase.flushToFile();
    }

    @AfterClass(groups = "perf")
    public void afterClass() {
        perfTestBase.afterClass();
    }

    @Test(groups = "perf", enabled = true)
    public void testSubmit() throws Exception {
        List<List<List<ApplicationId>>> appIdsPerRuns;
        System.out.println("Test 1: ");

        appIdsPerRuns = longRun("A");
        customerJobsToAppIdMap.put("A", appIdsPerRuns);
        System.out.println("        Customer A submits Analytic Run");

        appIdsPerRuns = longRun("B");
        customerJobsToAppIdMap.put("B", appIdsPerRuns);
        System.out.println("        Customer B submits Analytic Run");

        appIdsPerRuns = longRun("C");
        customerJobsToAppIdMap.put("C", appIdsPerRuns);
        System.out.println("        Customer C submits Analytic Run");

        appIdsPerRuns = longRun("D");
        customerJobsToAppIdMap.put("D", appIdsPerRuns);
        System.out.println("        Customer D submits Analytic Run");

        appIdsPerRuns = longRun("E");
        customerJobsToAppIdMap.put("E", appIdsPerRuns);
        System.out.println("        Customer E submits Analytic Run");

        dumpAppIdsToFile(customerJobsToAppIdMap);
        generateRunReport(customerJobsToAppIdMap);
    }

    @Test(groups = "perf", enabled = false)
    public void testSubmit2() throws Exception {
        List<List<List<ApplicationId>>> appIdsPerRuns;

        System.out.println("Test 2: ");
        // A
        for (int i = 0; i < 9; i++) {
            appIdsPerRuns = shortRun("A");
            List<List<List<ApplicationId>>> appIdsAllRuns = customerJobsToAppIdMap.get("A");
            if (appIdsAllRuns == null) {
                customerJobsToAppIdMap.put("A", appIdsPerRuns);
            } else {
                appIdsAllRuns.addAll(appIdsPerRuns);
            }

            System.out.println("        Customer A submits Analytic Run Short at " + i + " second" + (i > 0 ? "s" : ""));
            Thread.sleep(1000L);
        }
        Thread.sleep(7000L);

        // B
        appIdsPerRuns = shortRun("B");
        customerJobsToAppIdMap.put("B", appIdsPerRuns);
        System.out.println("        Customer B submits Analytic Run Short at 15 seconds");
        Thread.sleep(5000L);

        // C
        appIdsPerRuns = shortRun("C");
        customerJobsToAppIdMap.put("C", appIdsPerRuns);
        System.out.println("        Customer C submits Analytic Run Short at 20 seconds");

        dumpAppIdsToFile(customerJobsToAppIdMap);
        generateRunReport(customerJobsToAppIdMap);
    }

    @Test(groups = "perf", enabled = false)
    public void testSubmit3() throws Exception {
        List<List<List<ApplicationId>>> appIdsPerRuns;

        System.out.println("Test 3: ");

        // Customer A, B, C, D, E, F, G
        for (char customer = 'A', index = 0; customer < 'I'; customer++, index++) {
            appIdsPerRuns = longRun(String.valueOf(customer));
            customerJobsToAppIdMap.put(String.valueOf(customer), appIdsPerRuns);
            if (index == 0) {
                System.out.println("		Customer " + customer + " submits Analytic Run ");
            } else {
                System.out.println("		Customer " + customer + " submits Analytic Run at " + index + " second"
                        + (index == 1 ? "" : "s"));
            }
            Thread.sleep(1000L);
        }

        // Customer I
        appIdsPerRuns = longRun("A");
        customerJobsToAppIdMap.put(String.valueOf("I"), appIdsPerRuns);
        System.out.println("		Customer I submits Analytic Run at 8 seconds ");
        Thread.sleep(1000L);

        dumpAppIdsToFile(customerJobsToAppIdMap);
        generateRunReport(customerJobsToAppIdMap);
    }

    @Test(groups = "perf", enabled = false)
    public void generateTestRunReport() throws Exception {
        // after each test run, all jobIds are dumped to a temp file
        // you can find the file name from test rule console output
        // use this test helper method to reload them and monitor the progress
        String filePath = "/tmp/application-id9097432288354550211.log";
        Map<String, List<List<List<ApplicationId>>>> jobsToAppIdMap = loadAppIdsFromFile(filePath);
        generateRunReport(jobsToAppIdMap);
    }

    private Map<String, List<List<List<ApplicationId>>>> loadAppIdsFromFile(String filePath) throws IOException {
        Map<String, List<List<List<ApplicationId>>>> appIdsMap = new HashMap<String, List<List<List<ApplicationId>>>>();

        BufferedReader br = new BufferedReader(new FileReader(filePath));
        try {
            String line = br.readLine();

            while (line != null) {
                if (StringUtils.isEmpty(line)) {
                    line = br.readLine();
                    continue;
                }
                String customer;
                String[] rawData = line.split(":");
                customer = rawData[0];
                List<List<List<ApplicationId>>> runs = new ArrayList<List<List<ApplicationId>>>();
                appIdsMap.put(customer, runs);
                for (int runIndex = 1; runIndex < rawData.length; runIndex++) {
                    List<List<ApplicationId>> queues = new ArrayList<List<ApplicationId>>();
                    runs.add(queues);
                    for (String appIdsStr : rawData[runIndex].split("#")) {
                        List<ApplicationId> appIds = new ArrayList<ApplicationId>();
                        queues.add(appIds);
                        for (String appIdStr : appIdsStr.split(",")) {
                            String[] appIdSplit = appIdStr.split("_");
                            appIds.add(ApplicationId.newInstance(Long.valueOf(appIdSplit[1]),
                                    Integer.valueOf(appIdSplit[2])));
                        }
                    }
                }
                line = br.readLine();
            }
        } finally {
            br.close();
        }
        return appIdsMap;
    }

    private void dumpAppIdsToFile(Map<String, List<List<List<ApplicationId>>>> jobsToAppIdMap) throws IOException {

        File tempRMLogFile = File.createTempFile("application-id", ".log");

        FileWriter fw = new FileWriter(tempRMLogFile);
        BufferedWriter bw = new BufferedWriter(fw);
        try {

            Iterator<String> customerIterator = jobsToAppIdMap.keySet().iterator();
            while (customerIterator.hasNext()) {
                String customer = customerIterator.next();
                bw.write(customer + ":");
                int runIndex = 0;
                for (List<List<ApplicationId>> run : jobsToAppIdMap.get(customer)) {
                    bw.write(runIndex > 0 ? ":" : "");
                    int queueIndex = 0;
                    for (List<ApplicationId> appIdsPerQueue : run) {
                        bw.write(queueIndex > 0 ? "#" : "");
                        int appIdIndex = 0;
                        for (ApplicationId appId : appIdsPerQueue) {
                            bw.write((appIdIndex > 0 ? "," : "") + appId);
                            appIdIndex++;
                        }
                        queueIndex++;
                    }
                    runIndex++;
                }
                bw.write("\n");
            }
            System.out.println("Generated submitted AppIds " + tempRMLogFile.getAbsolutePath());
        } finally {
            bw.close();
        }
    }

    private Properties[] getPropertiesPair(Classifier classifier, String queue) {
        Properties containerProperties = new Properties();
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "1024");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");
        containerProperties.put(ContainerProperty.METADATA.name(), classifier.toString());
        
        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), queue);
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), "Dell");

        return new Properties[] { appMasterProperties, containerProperties };
    }

    private List<List<List<ApplicationId>>> shortRun(String queue) {
        return run(queue, false);
    }

    private List<List<List<ApplicationId>>> longRun(String queue) {
        return run(queue, true);
    }

    private List<List<List<ApplicationId>>> run(String queue, boolean isLong) {
        List<ApplicationId> appIdsPerQueue = new ArrayList<ApplicationId>();
        List<List<ApplicationId>> appIdsList = new ArrayList<List<ApplicationId>>();
        Properties[] p0 = getPropertiesPair(classifier1Min, "Priority0." + queue);
        appIdsList.add(appIdsPerQueue);
        appIdsPerQueue.add(jobService.submitYarnJob("pythonClient", p0[0], p0[1]));

        appIdsPerQueue = new ArrayList<ApplicationId>();
        appIdsList.add(appIdsPerQueue);

        /*
        for (int j = 0; j < 2; j++) {
            Properties[] p1 = getPropertiesPair(classifier5Mins, "Priority1." + queue);
            appIdsPerQueue.add(jobService.submitYarnJob("pythonClient", p1[0], p1[1]));
        }

        if (isLong) {
            appIdsPerQueue = new ArrayList<ApplicationId>();
            appIdsList.add(appIdsPerQueue);
            for (int j = 0; j < 8; j++) {
                Properties[] p1 = getPropertiesPair(classifier10Mins, "Priority2." + queue);
                appIdsPerQueue.add(jobService.submitYarnJob("pythonClient", p1[0], p1[1]));
            }
        }*/

        List<List<List<ApplicationId>>> appIdsPerRunList = new ArrayList<List<List<ApplicationId>>>();

        appIdsPerRunList.add(appIdsList);
        return appIdsPerRunList;
    }

    private Map<ApplicationId, ApplicationReport> waitForAllJobsToFinish(
            Map<String, List<List<List<ApplicationId>>>> jobsToAppIdMap) throws Exception {

        List<ApplicationId> appIds = getAllRunningAppIds(jobsToAppIdMap);

        Map<ApplicationId, ApplicationReport> jobStatus = new HashMap<ApplicationId, ApplicationReport>();
        List<ApplicationId> jobStatusToCollect = new ArrayList<ApplicationId>(appIds);

        long startTime = System.currentTimeMillis();
        long nextReportTime = 60000L;
        while (!jobStatusToCollect.isEmpty()) {
            ApplicationId appId = jobStatusToCollect.get(0);
            YarnApplicationState state = waitState(appId, 30, TimeUnit.SECONDS, YarnApplicationState.FAILED,
                    YarnApplicationState.FINISHED);
            if (state == null) {
                System.out.println("ERROR: Invalid State detected");
                jobStatusToCollect.remove(appId);
                continue;
            }
            if (state.equals(YarnApplicationState.FAILED) || state.equals(YarnApplicationState.FINISHED)) {
                jobStatusToCollect.remove(appId);
                jobStatus.put(appId, jobService.getJobReportById(appId));
            }
            long runningTime = System.currentTimeMillis() - startTime;
            if (runningTime > nextReportTime) {
                System.out.println("\nReport status after " + runningTime / 1000 + " seconds");
                reportAllJobsStatus(null, null, null, jobsToAppIdMap, true);
                nextReportTime += 60000L;
            }
        }
        return jobStatus;
    }

    private List<ApplicationId> getAllRunningAppIds(Map<String, List<List<List<ApplicationId>>>> jobsToAppIdMap) {
        List<ApplicationId> appIds = new ArrayList<ApplicationId>();

        Iterator<String> customerIterator = jobsToAppIdMap.keySet().iterator();
        while (customerIterator.hasNext()) {
            String customer = customerIterator.next();
            for (List<List<ApplicationId>> run : jobsToAppIdMap.get(customer)) {
                for (List<ApplicationId> appIdsPerQueue : run) {
                    appIds.addAll(appIdsPerQueue);
                }
            }
        }
        return appIds;
    }

    private void reportAllJobsStatus(Map<ApplicationId, ApplicationReport> jobReport,
            Map<String, Date> jobRunStartTime, Map<String, List<Double>> queueWaitTimes,
            Map<String, List<List<List<ApplicationId>>>> jobsToAppIdMap, boolean showStatusOnly) {

        Iterator<String> customerIterator = jobsToAppIdMap.keySet().iterator();
        while (customerIterator.hasNext()) {
            String customer = customerIterator.next();
            System.out.println("	Customer " + customer);
            int runIndex = 1;
            for (List<List<ApplicationId>> run : jobsToAppIdMap.get(customer)) {
                System.out.println("		Analytic Run " + runIndex++);
                int priorityIndex = 0;
                for (List<ApplicationId> appIdsPerQueue : run) {
                    System.out.println("			Priority " + priorityIndex++);
                    if (showStatusOnly) {
                        for (ApplicationId appId : appIdsPerQueue) {
                            ApplicationReport report = jobService.getJobReportById(appId);
                            if (report.getYarnApplicationState() == YarnApplicationState.FINISHED) {
                                double runTime = (report.getFinishTime() - report.getStartTime()) / 1000.0;
                                System.out.println("				" + appId + " state " + report.getYarnApplicationState()
                                        + " FinalStatus " + report.getFinalApplicationStatus() + " totalRunTime "
                                        + runTime + " seconds");
                            } else {
                                System.out.println("				" + appId + " state " + report.getYarnApplicationState()
                                        + " FinalStatus " + report.getFinalApplicationStatus());
                            }
                        }
                    } else {
                        reportRunStatistic(jobReport, jobRunStartTime, queueWaitTimes, appIdsPerQueue);
                    }
                }
            }
        }
    }

    private void reportRunStatistic(Map<ApplicationId, ApplicationReport> jobReport,
            Map<String, Date> jobRunStartTime, Map<String, List<Double>> queueWaitTimes, List<ApplicationId> appIds) {
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
        formatter.setTimeZone(TimeZone.getTimeZone("EST"));
        for (ApplicationId appId : appIds) {
            ApplicationReport report = jobReport.get(appId);
            double elapsedTime = (report.getFinishTime() - report.getStartTime()) / 1000.0;
            String startTime = formatter.format(new Date(report.getStartTime()));
            String endTime = formatter.format(new Date(report.getFinishTime()));
            Date runStartTime = jobRunStartTime.get(appId.toString());
            double waitTime = runStartTime == null ? -1.0
                    : (runStartTime.getTime() - report.getStartTime() - 10800000) / 1000.0;
            List<Double> items = queueWaitTimes.get(report.getQueue());
            if (items == null) {
                items = new ArrayList<Double>();
                items.add(waitTime);
                if (waitTime == -1) {
                    System.out.println("ERROR: cannot find job " + appId + " start time");
                }
                queueWaitTimes.put(report.getQueue(), items);
            } else {
                items.add(waitTime);
                if (waitTime == -1) {
                    System.out.println("ERROR: cannot find job " + appId + " start time");
                }
            }
            System.out.println("					" + appId + " - submitTime: " + startTime + " - finishTime: " + endTime
                    + " - elapsed: " + elapsedTime + " seconds " + " - waitTime: " + waitTime + " seconds");
        }
    }

    private void generateRunReport(Map<String, List<List<List<ApplicationId>>>> jobsToAppIdMap) throws Exception {

        waitForAllJobsToFinish(jobsToAppIdMap);
    }

}
