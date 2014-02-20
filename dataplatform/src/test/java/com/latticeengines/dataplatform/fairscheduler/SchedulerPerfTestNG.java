package com.latticeengines.dataplatform.fairscheduler;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.domain.Classifier;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.functionalframework.SecureFileTransferAgent;
import com.latticeengines.dataplatform.functionalframework.SecureFileTransferAgent.FileTransferOption;
import com.latticeengines.dataplatform.service.JobService;

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
    private SecureFileTransferAgent secureFileTransferAgent;

    @Value("${dataplatform.yarn.resourcemanager.fairscheduler.xml.location}")
    private String remoteFairSchedulerFilePath;

    @Value("${dataplatform.yarn.resourcemanager.log.location}")
    private String remoteRMLogPath;

    private Classifier classifier1Min;
    private Classifier classifier5Mins;
    private Classifier classifier10Mins;
    private Map<String, List<List<List<ApplicationId>>>> customerJobsToAppIdMap = new HashMap<String, List<List<List<ApplicationId>>>>();

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
        classifier1Min = new Classifier();
        classifier1Min.setName("IrisClassifier");
        classifier1Min
                .setFeatures(Arrays.<String> asList(new String[] {
                        "sepal_length", "sepal_width", "petal_length",
                        "petal_width" }));
        classifier1Min.setTargets(Arrays
                .<String> asList(new String[] { "category" }));
        classifier1Min.setSchemaHdfsPath("/scheduler/iris.json");
        classifier1Min.setModelHdfsDir("/scheduler/result");
        classifier1Min.setPythonScriptHdfsPath("/scheduler/train_1min.py");
        classifier1Min.setTrainingDataHdfsPath("/training/train.dat");
        classifier1Min.setTestDataHdfsPath("/test/test.dat");

        classifier5Mins = new Classifier();
        classifier5Mins.setName("IrisClassifier");
        classifier5Mins
                .setFeatures(Arrays.<String> asList(new String[] {
                        "sepal_length", "sepal_width", "petal_length",
                        "petal_width" }));
        classifier5Mins.setTargets(Arrays
                .<String> asList(new String[] { "category" }));
        classifier5Mins.setSchemaHdfsPath("/scheduler/iris.json");
        classifier5Mins.setModelHdfsDir("/scheduler/result");
        classifier5Mins.setPythonScriptHdfsPath("/scheduler/train_5mins.py");
        classifier5Mins.setTrainingDataHdfsPath("/training/train.dat");
        classifier5Mins.setTestDataHdfsPath("/test/test.dat");

        classifier10Mins = new Classifier();
        classifier10Mins.setName("IrisClassifier");
        classifier10Mins
                .setFeatures(Arrays.<String> asList(new String[] {
                        "sepal_length", "sepal_width", "petal_length",
                        "petal_width" }));
        classifier10Mins.setTargets(Arrays
                .<String> asList(new String[] { "category" }));
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
        URL trainingFileUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/exposed/service/impl/train.dat");
        URL testFileUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/exposed/service/impl/test.dat");
        URL jsonUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/exposed/service/impl/iris.json");
        URL train1MinUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/fairscheduler/train_1min.py");
        URL train5MinsUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/fairscheduler/train_5mins.py");
        URL train10MinsUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/fairscheduler/train_10mins.py");

        String trainingFilePath = "file:" + trainingFileUrl.getFile();
        String testFilePath = "file:" + testFileUrl.getFile();
        String jsonFilePath = "file:" + jsonUrl.getFile();
        String train1MinScriptPath = "file:" + train1MinUrl.getFile();
        String train5MinsScriptPath = "file:" + train5MinsUrl.getFile();
        String train10MinsScriptPath = "file:" + train10MinsUrl.getFile();

        copyEntries.add(new CopyEntry(trainingFilePath, "/training", false));
        copyEntries.add(new CopyEntry(testFilePath, "/test", false));
        copyEntries.add(new CopyEntry(jsonFilePath, "/scheduler", false));
        copyEntries
                .add(new CopyEntry(train1MinScriptPath, "/scheduler", false));
        copyEntries
                .add(new CopyEntry(train5MinsScriptPath, "/scheduler", false));
        copyEntries.add(new CopyEntry(train10MinsScriptPath, "/scheduler",
                false));

        doCopy(fs, copyEntries);
    }

    @AfterMethod
    private void cleanUp() {
        customerJobsToAppIdMap.clear();
    }

    @Test(groups = "perf", enabled = false)
    public void testSubmit() throws Exception {
        List<List<List<ApplicationId>>> appIdsPerRuns;
        System.out.println("Test 1: ");

        appIdsPerRuns = longRun("A");
        customerJobsToAppIdMap.put("A", appIdsPerRuns);
        System.out.println("		Customer A submits Analytic Run ");
        Thread.sleep(20000L);

        appIdsPerRuns = longRun("B");
        customerJobsToAppIdMap.put("B", appIdsPerRuns);
        System.out
                .println("		Customer B submits Analytic Run 20 seconds later ");

        dumpAppIdsToFile(customerJobsToAppIdMap);
        generateRunReport(customerJobsToAppIdMap);
    }

    @Test(groups = "perf", enabled = true)
    public void testSubmit2() throws Exception {
        List<List<List<ApplicationId>>> appIdsPerRuns;

        System.out.println("Test 2: ");
        // A
        for (int i = 0; i < 9; i++) {
            appIdsPerRuns = shortRun("A");
            List<List<List<ApplicationId>>> appIdsAllRuns = customerJobsToAppIdMap
                    .get("A");
            if (appIdsAllRuns == null) {
                customerJobsToAppIdMap.put("A", appIdsPerRuns);
            } else {
                appIdsAllRuns.addAll(appIdsPerRuns);
            }

            System.out.println("		Customer A submits Analytic Run Short at "
                    + i + " second" + (i > 0 ? "s" : ""));
            Thread.sleep(1000L);
        }
        Thread.sleep(7000L);

        // B
        appIdsPerRuns = shortRun("B");
        customerJobsToAppIdMap.put("B", appIdsPerRuns);
        System.out
                .println("		Customer B submits Analytic Run Short at 15 seconds");
        Thread.sleep(5000L);

        // C
        appIdsPerRuns = shortRun("C");
        customerJobsToAppIdMap.put("C", appIdsPerRuns);
        System.out
                .println("		Customer C submits Analytic Run Short at 20 seconds");

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
                System.out.println("		Customer " + customer
                        + " submits Analytic Run ");
            } else {
                System.out.println("		Customer " + customer
                        + " submits Analytic Run at " + index + " second"
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
        try {
            Map<String, List<List<List<ApplicationId>>>> jobsToAppIdMap = loadAppIdsFromFile(filePath);
            generateRunReport(jobsToAppIdMap);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Map<String, List<List<List<ApplicationId>>>> loadAppIdsFromFile(
            String filePath) throws IOException {
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
                            appIds.add(ApplicationId.newInstance(
                                    Long.valueOf(appIdSplit[1]),
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

    private void dumpAppIdsToFile(
            Map<String, List<List<List<ApplicationId>>>> jobsToAppIdMap)
            throws IOException {

        File tempRMLogFile = File.createTempFile("application-id", ".log");

        FileWriter fw = new FileWriter(tempRMLogFile);
        BufferedWriter bw = new BufferedWriter(fw);
        try {

            Iterator<String> customerIterator = jobsToAppIdMap.keySet()
                    .iterator();
            while (customerIterator.hasNext()) {
                String customer = customerIterator.next();
                bw.write(customer + ":");
                int runIndex = 0;
                for (List<List<ApplicationId>> run : jobsToAppIdMap
                        .get(customer)) {
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
            System.out.println("Generated submitted AppIds "
                    + tempRMLogFile.getAbsolutePath());
        } finally {
            bw.close();
        }
    }

    private Properties[] getPropertiesPair(Classifier classifier, String queue) {
        Properties containerProperties = new Properties();
        containerProperties.put("VIRTUALCORES", "1");
        containerProperties.put("MEMORY", "1024");
        containerProperties.put("PRIORITY", "0");
        containerProperties.put("METADATA", classifier.toString());

        Properties appMasterProperties = new Properties();
        appMasterProperties.put("QUEUE", queue);

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
        Properties[] p0 = getPropertiesPair(classifier1Min, "Priority0."
                + queue);
        appIdsList.add(appIdsPerQueue);
        appIdsPerQueue.add(jobService.submitYarnJob("pythonClient", p0[0],
                p0[1]));

        appIdsPerQueue = new ArrayList<ApplicationId>();
        appIdsList.add(appIdsPerQueue);
        for (int j = 0; j < 2; j++) {
            Properties[] p1 = getPropertiesPair(classifier5Mins, "Priority1."
                    + queue);
            appIdsPerQueue.add(jobService.submitYarnJob("pythonClient", p1[0],
                    p1[1]));
        }

        if (isLong) {
            appIdsPerQueue = new ArrayList<ApplicationId>();
            appIdsList.add(appIdsPerQueue);
            for (int j = 0; j < 8; j++) {
                Properties[] p1 = getPropertiesPair(classifier10Mins,
                        "Priority2." + queue);
                appIdsPerQueue.add(jobService.submitYarnJob("pythonClient",
                        p1[0], p1[1]));
            }
        }

        List<List<List<ApplicationId>>> appIdsPerRunList = new ArrayList<List<List<ApplicationId>>>();

        appIdsPerRunList.add(appIdsList);
        return appIdsPerRunList;
    }

    private Map<ApplicationId, ApplicationReport> waitForAllJobToFinsih(
            Map<String, List<List<List<ApplicationId>>>> jobsToAppIdMap)
            throws Exception {

        List<ApplicationId> appIds = getAllRunningAppIds(jobsToAppIdMap);

        Map<ApplicationId, ApplicationReport> jobStatus = new HashMap<ApplicationId, ApplicationReport>();
        List<ApplicationId> jobStatusToCollect = new ArrayList<ApplicationId>(
                appIds);

        long startTime = System.currentTimeMillis();
        long nextReportTime = 60000L;
        while (!jobStatusToCollect.isEmpty()) {
            ApplicationId appId = jobStatusToCollect.get(0);
            YarnApplicationState state = waitState(appId, 30, TimeUnit.SECONDS,
                    YarnApplicationState.FAILED, YarnApplicationState.FINISHED);
            if (state == null) {
                System.out.println("ERROR: Invalid State detected");
                jobStatusToCollect.remove(appId);
                continue;
            }
            if (state.equals(YarnApplicationState.FAILED)
                    || state.equals(YarnApplicationState.FINISHED)) {
                jobStatusToCollect.remove(appId);
                jobStatus.put(appId, jobService.getJobReportById(appId));
            }
            long runningTime = System.currentTimeMillis() - startTime;
            if (runningTime > nextReportTime) {
                System.out.println("\nReport status after " + runningTime
                        / 1000 + " seconds");
                reportAllJobsStatus(null, null, null, jobsToAppIdMap, true);
                nextReportTime += 60000L;
            }
        }
        return jobStatus;
    }

    private List<ApplicationId> getAllRunningAppIds(
            Map<String, List<List<List<ApplicationId>>>> jobsToAppIdMap) {
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

    private void reportAllJobsStatus(
            Map<ApplicationId, ApplicationReport> jobReport,
            Map<String, Date> jobRunStartTime,
            Map<String, List<Double>> queueWaitTimes,
            Map<String, List<List<List<ApplicationId>>>> jobsToAppIdMap,
            boolean showStatusOnly) {

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
                            ApplicationReport report = jobService
                                    .getJobReportById(appId);
                            if (report.getYarnApplicationState() == YarnApplicationState.FINISHED) {
                                double runTime = (report.getFinishTime() - report
                                        .getStartTime()) / 1000.0;
                                System.out.println("				" + appId + " state "
                                        + report.getYarnApplicationState()
                                        + " FinalStatus "
                                        + report.getFinalApplicationStatus()
                                        + " totalRunTime " + runTime
                                        + " seconds");
                            } else {
                                System.out.println("				" + appId + " state "
                                        + report.getYarnApplicationState()
                                        + " FinalStatus "
                                        + report.getFinalApplicationStatus());
                            }
                        }
                    } else {
                        reportRunStatisitic(jobReport, jobRunStartTime,
                                queueWaitTimes, appIdsPerQueue);
                    }
                }
            }
        }
    }

    private void reportRunStatisitic(
            Map<ApplicationId, ApplicationReport> jobReport,
            Map<String, Date> jobRunStartTime,
            Map<String, List<Double>> queueWaitTimes, List<ApplicationId> appIds) {
        SimpleDateFormat formatter = new SimpleDateFormat(
                "yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
        formatter.setTimeZone(TimeZone.getTimeZone("EST"));
        for (ApplicationId appId : appIds) {
            ApplicationReport report = jobReport.get(appId);
            double elapsedTime = (report.getFinishTime() - report
                    .getStartTime()) / 1000.0;
            String startTime = formatter
                    .format(new Date(report.getStartTime()));
            String endTime = formatter.format(new Date(report.getFinishTime()));
            Date runStartTime = jobRunStartTime.get(appId.toString());
            double waitTime = runStartTime == null ? -1.0 : (runStartTime
                    .getTime() - report.getStartTime() - 10800000) / 1000.0;
            List<Double> items = queueWaitTimes.get(report.getQueue());
            if (items == null) {
                items = new ArrayList<Double>();
                items.add(waitTime);
                if (waitTime == -1) {
                    System.out.println("ERROR: cannot find job " + appId
                            + " start time");
                }
                queueWaitTimes.put(report.getQueue(), items);
            } else {
                items.add(waitTime);
                if (waitTime == -1) {
                    System.out.println("ERROR: cannot find job " + appId
                            + " start time");
                }
            }
            System.out.println("					" + appId + " - submitTime: " + startTime
                    + " - finishTime: " + endTime + " - elapsed: "
                    + elapsedTime + " seconds " + " - waitTime: " + waitTime
                    + " seconds");
        }
    }

    private void generateRunReport(
            Map<String, List<List<List<ApplicationId>>>> jobsToAppIdMap)
            throws Exception {

        Map<ApplicationId, ApplicationReport> jobReport = waitForAllJobToFinsih(jobsToAppIdMap);
        Map<String, Date> jobRunStartTime = collectStatistics(jobsToAppIdMap);
        Map<String, List<Double>> queueWaitTimes = new HashMap<String, List<Double>>();

        System.out.println("Report all job status ");
        reportAllJobsStatus(jobReport, jobRunStartTime, queueWaitTimes,
                jobsToAppIdMap, false);

        System.out.println("\n Qeueue Statistics");
        Iterator<String> iterator = queueWaitTimes.keySet().iterator();
        while (iterator.hasNext()) {
            String queue = iterator.next();
            List<Double> items = queueWaitTimes.get(queue);
            Collections.sort(items);
            double totalWaitTime = 0.0;
            for (Double waitTime : items) {
                totalWaitTime += waitTime;
            }
            int totalJobs = items.size();
            int mediaIndex = totalJobs / 2;
            System.out.println("	Queue: " + queue);
            System.out.println("		total jobs run: " + totalJobs);
            System.out.println("		average wait time: " + totalWaitTime
                    / items.size() + " seconds");
            System.out.println("		median wait time (" + mediaIndex + "): "
                    + items.get(mediaIndex) + " seconds");
            System.out.println("		min wait time: " + items.get(0) + " seconds");
            System.out.println("		max wait time: " + items.get(totalJobs - 1)
                    + " seconds");
        }
    }

    private Map<String, Date> collectStatistics(
            Map<String, List<List<List<ApplicationId>>>> jobsToAppIdMap)
            throws IOException, FileNotFoundException, ParseException {

        File tempRMLogFile = File.createTempFile("resource-manager", ".log");

        secureFileTransferAgent.fileTranser(tempRMLogFile.getAbsolutePath(),
                remoteRMLogPath, FileTransferOption.DOWNLOAD);

        List<ApplicationId> applicationIds = getAllRunningAppIds(jobsToAppIdMap);
        Map<String, Date> runStartTimestamp = new HashMap<String, Date>();
        for (ApplicationId appId : applicationIds) {
            runStartTimestamp.put(appId.toString(), null);
        }

        BufferedReader br = new BufferedReader(new FileReader(tempRMLogFile));
        try {
            String line = br.readLine();
            Pattern pattern = Pattern
                    .compile("application_.+?State change from ACCEPTED to RUNNING");
            Matcher matcher = null;

            while (line != null) {
                String appIdStr;
                String timestampStr;
                matcher = pattern.matcher(line);
                if (matcher.find()) {
                    appIdStr = matcher.group();
                    appIdStr = appIdStr.substring(0, appIdStr.indexOf(' '));
                    if (runStartTimestamp.containsKey(appIdStr)) {
                        timestampStr = line.substring(0, 23);
                        Date date = new SimpleDateFormat(
                                "yyyy-MM-dd HH:mm:ss,SSS", Locale.ENGLISH)
                                .parse(timestampStr);
                        runStartTimestamp.put(appIdStr, date);
                    }
                }
                line = br.readLine();
            }
            tempRMLogFile.delete();
        } finally {
            br.close();
        }
        return runStartTimestamp;
    }

}
