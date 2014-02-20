package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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

import com.latticeengines.dataplatform.exposed.domain.Classifier;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.runtime.execution.python.PythonContainerProperty;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.util.HdfsHelper;
import com.latticeengines.dataplatform.yarn.client.ContainerProperty;

@ContextConfiguration(locations = { "classpath:com/latticeengines/dataplatform/service/impl/JobServiceImplTestNG-context.xml" })
public class JobServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private JobService jobService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        new File("/tmp/ledpjob-metrics.out").delete();
        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.delete(new Path("/training"), true);
        fs.delete(new Path("/test"), true);
        fs.delete(new Path("/datascientist1"), true);

        fs.mkdirs(new Path("/training"));
        fs.mkdirs(new Path("/test"));
        fs.mkdirs(new Path("/datascientist1"));

        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
        URL trainingFileUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/service/impl/nn_train.dat");
        URL testFileUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/service/impl/nn_test.dat");
        URL jsonUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/service/impl/iris.json");
        URL pythonScriptUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/service/impl/nn_train.py");
        URL modelUrl = ClassLoader
                .getSystemResource("com/latticeengines/dataplatform/service/impl/model.txt");
        String trainingFilePath = "file:" + trainingFileUrl.getFile();
        String testFilePath = "file:" + testFileUrl.getFile();
        String jsonFilePath = "file:" + jsonUrl.getFile();
        String pythonScriptPath = "file:" + pythonScriptUrl.getFile();
        FileUtils.copyFileToDirectory(new File(modelUrl.getFile()), new File(
                "/tmp"));
        copyEntries.add(new CopyEntry(trainingFilePath, "/training", false));
        copyEntries.add(new CopyEntry(testFilePath, "/test", false));
        copyEntries.add(new CopyEntry(jsonFilePath, "/datascientist1", false));
        copyEntries.add(new CopyEntry(pythonScriptPath, "/datascientist1",
                false));
        doCopy(fs, copyEntries);
    }

    @Test(groups = "functional", enabled = false)
    public void testGetJobReportsAll() throws Exception {
        List<ApplicationReport> applications = jobService.getJobReportsAll();
        assertNotNull(applications);
    }

    @Test(groups = "functional", enabled = false)
    public void testKillApplication() throws Exception {
        Properties appMasterProperties = new Properties();
        Properties containerProperties = new Properties();
        containerProperties.put("VIRTUALCORES", "1");
        containerProperties.put("MEMORY", "64");
        containerProperties.put("PRIORITY", "0");
        ApplicationId applicationId = jobService.submitYarnJob(
                "defaultYarnClient", appMasterProperties, containerProperties);
        YarnApplicationState state = waitState(applicationId, 30,
                TimeUnit.SECONDS, YarnApplicationState.RUNNING);
        assertNotNull(state);
        jobService.killJob(applicationId);
        state = getState(applicationId);
        assertNotNull(state);
        assertTrue(state.equals(YarnApplicationState.KILLED));
    }

    @Test(groups = "functional", enabled = false)
    public void testGetJobReportByUser() throws Exception {
        Properties appMasterProperties = new Properties();
        Properties containerProperties = new Properties();
        containerProperties.put("VIRTUALCORES", "1");
        containerProperties.put("MEMORY", "64");
        containerProperties.put("PRIORITY", "0");
        ApplicationId applicationId = jobService.submitYarnJob(
                "defaultYarnClient", appMasterProperties, containerProperties);
        YarnApplicationState state = waitState(applicationId, 30,
                TimeUnit.SECONDS, YarnApplicationState.RUNNING);
        assertNotNull(state);
        jobService.killJob(applicationId);
        state = getState(applicationId);
        assertNotNull(state);
        assertTrue(state.equals(YarnApplicationState.KILLED));

        ApplicationReport app = jobService.getJobReportById(applicationId);

        List<ApplicationReport> reports = jobService.getJobReportByUser(app
                .getUser());
        int numJobs = reports.size();
        assertTrue(numJobs > 0);

        applicationId = jobService.submitYarnJob("defaultYarnClient",
                appMasterProperties, containerProperties);

        state = waitState(applicationId, 10, TimeUnit.SECONDS,
                YarnApplicationState.RUNNING);
        reports = jobService.getJobReportByUser(app.getUser());
        assertTrue(reports.size() > numJobs);
        jobService.killJob(applicationId);

    }

    @Test(groups = "functional")
    public void testSubmitPythonYarnJob() throws Exception {
        Classifier classifier = new Classifier();
        classifier.setName("IrisClassifier");
        classifier
                .setFeatures(Arrays.<String> asList(new String[] {
                        "sepal_length", "sepal_width", "petal_length",
                        "petal_width" }));
        classifier.setTargets(Arrays
                .<String> asList(new String[] { "category" }));
        classifier.setSchemaHdfsPath("/datascientist1/iris.json");
        classifier.setModelHdfsDir("/datascientist1/result");
        classifier.setPythonScriptHdfsPath("/datascientist1/nn_train.py");
        classifier.setTrainingDataHdfsPath("/training/nn_train.dat");
        classifier.setTestDataHdfsPath("/test/nn_test.dat");

        Properties appMasterProperties = new Properties();
        appMasterProperties.put("QUEUE", "Priority0.A");

        Properties containerProperties = new Properties();
        containerProperties.put("VIRTUALCORES", "1");
        containerProperties.put("MEMORY", "64");
        containerProperties.put("PRIORITY", "0");
        containerProperties.put("METADATA", classifier.toString());

        ApplicationId applicationId = jobService.submitYarnJob("pythonClient",
                appMasterProperties, containerProperties);
        YarnApplicationState state = waitState(applicationId, 30,
                TimeUnit.SECONDS, YarnApplicationState.RUNNING);
        assertNotNull(state);
        ApplicationReport app = jobService.getJobReportById(applicationId);
        assertNotNull(app);
        state = waitState(applicationId, 120, TimeUnit.SECONDS,
                YarnApplicationState.FINISHED);
        assertEquals(state, YarnApplicationState.FINISHED);

        NumberFormat appIdFormat = getAppIdFormat();
        String jobId = applicationId.getClusterTimestamp() + "_"
                + appIdFormat.format(applicationId.getId());
        String modelFile = HdfsHelper.getFilesForDir(yarnConfiguration,
                "/datascientist1/result/" + jobId).get(0);
        String modelContents = HdfsHelper.getHdfsFileContents(
                yarnConfiguration, modelFile);
        assertEquals(modelContents.trim(), "this is the generated model.");

        String contextFileName = containerProperties
                .getProperty(ContainerProperty.APPMASTER_CONTEXT_FILE.name());
        String metadataFileName = containerProperties
                .getProperty(PythonContainerProperty.METADATA.name());

        assertFalse(new File(contextFileName).exists());
        assertFalse(new File(metadataFileName).exists());
    }

    @Test(groups = "functional", enabled = false)
    public void testSubmitMRJob() throws Exception {

        Configuration conf = (Configuration) applicationContext
                .getBean("hadoopConfiguration");
        FileSystem fileSystem = null;
        FSDataOutputStream fileOut = null;
        try {
            fileSystem = FileSystem.get(conf);
            String dir = "/output";
            Path path = new Path(dir);
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);
                System.out.println("Deleted dir " + dir);
            }

            Path inputFilepath = new Path("/input/file1.txt");
            if (fileSystem.exists(inputFilepath)) {
                fileSystem.delete(inputFilepath, true);
            }

            // Create a new file and write data to it.
            fileOut = fileSystem.create(inputFilepath);
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                    fileOut));
            writer.write("Watson is awesome\n");
            writer.flush();
            fileOut.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fileOut.close();
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        ApplicationId applicationId = jobService.submitMRJob("wordCountJob",
                null);
        YarnApplicationState state = waitState(applicationId, 120,
                TimeUnit.SECONDS, YarnApplicationState.FINISHED);

        state = getState(applicationId);
        assertNotNull(state);
        assertTrue(!state.equals(YarnApplicationState.FAILED));

        ApplicationReport app = jobService.getJobReportById(applicationId);
        String log = HdfsHelper.getApplicationLog(yarnConfiguration,
                app.getUser(), applicationId.toString());
        assertTrue(!log.isEmpty());
    }

}
