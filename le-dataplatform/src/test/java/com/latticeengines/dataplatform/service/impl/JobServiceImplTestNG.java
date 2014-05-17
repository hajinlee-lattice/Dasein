package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.dataplatform.client.yarn.AppMasterProperty;
import com.latticeengines.dataplatform.client.yarn.ContainerProperty;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.runtime.mapreduce.EventDataSamplingProperty;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.dataplatform.service.JobNameService;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.domain.exposed.dataplatform.Classifier;
import com.latticeengines.domain.exposed.dataplatform.DbCreds;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.domain.exposed.dataplatform.SamplingElement;

public class JobServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private JobService jobService;
    
    @Autowired
    private Configuration hadoopConfiguration;
    
    @Autowired
    private JobNameService jobNameService;

    private String inputDir = null;
    private String outputDir = null;
    private SamplingConfiguration samplingConfig = null;
    
    @BeforeClass(groups = "functional")
    public void setupSamplingMRJob() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.delete(new Path("/eventTable"), true);
        fs.delete(new Path("/tmp/import"), true);

        inputDir = ClassLoader.getSystemResource("com/latticeengines/dataplatform/runtime/mapreduce/DELL_EVENT_TABLE").getPath();
        outputDir = inputDir + "/samples";
        FileUtils.deleteDirectory(new File(outputDir));
        samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        SamplingElement s0 = new SamplingElement();
        s0.setName("s0");
        s0.setPercentage(30);
        SamplingElement s1 = new SamplingElement();
        s1.setName("s1");
        s1.setPercentage(60);
        samplingConfig.addSamplingElement(s0);
        samplingConfig.addSamplingElement(s1);
        
        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
        
        File[] avroFiles = getAvroFilesForDir(inputDir);
        for (File avroFile : avroFiles) {
            copyEntries.add(new CopyEntry("file:" + avroFile.getAbsolutePath(), "/eventTable", false));
        }
        
        inputDir = "/eventTable";
        outputDir = inputDir + "/samples";
        doCopy(fs, copyEntries);
    }
    
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

        URL modelUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/service/impl/model.txt");

        String trainingFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_train.dat");
        String testFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_test.dat");
        String jsonFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/iris.json");
        String pythonScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_train.py");
        FileUtils.copyFileToDirectory(new File(modelUrl.getFile()), new File("/tmp"));
        copyEntries.add(new CopyEntry(trainingFilePath, "/training", false));
        copyEntries.add(new CopyEntry(testFilePath, "/test", false));
        copyEntries.add(new CopyEntry(jsonFilePath, "/datascientist1", false));
        copyEntries.add(new CopyEntry(pythonScriptPath, "/datascientist1", false));
        doCopy(fs, copyEntries);
    }

    private Properties createAppMasterPropertiesForYarnJob() {
        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), "Priority0.0");
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), "Dell");
        return appMasterProperties;
    }
    
    private Properties createContainerPropertiesForYarnJob() {
        Properties containerProperties = new Properties();
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "64");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");
        return containerProperties;
    }
    
    @Test(groups = "functional", enabled = true)
    public void testGetJobReportsAll() throws Exception {
        List<ApplicationReport> applications = jobService.getJobReportsAll();
        assertNotNull(applications);
    }

    @Test(groups = "functional", enabled = true)
    public void testKillApplication() throws Exception {        
        Properties appMasterProperties = createAppMasterPropertiesForYarnJob();

        Properties containerProperties = createContainerPropertiesForYarnJob();     
        
        ApplicationId applicationId = jobService.submitYarnJob("defaultYarnClient", appMasterProperties,
                containerProperties);
        YarnApplicationState state = waitState(applicationId, 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
        assertNotNull(state);
        jobService.killJob(applicationId);
        state = getState(applicationId);
        assertNotNull(state);
        assertTrue(state.equals(YarnApplicationState.KILLED));
    }

    @Test(groups = "functional", enabled = true)
    public void testGetJobReportByUser() throws Exception {        
        Properties appMasterProperties = createAppMasterPropertiesForYarnJob();

        Properties containerProperties = createContainerPropertiesForYarnJob();
        
        ApplicationId applicationId = jobService.submitYarnJob("defaultYarnClient", appMasterProperties,
                containerProperties);
        YarnApplicationState state = waitState(applicationId, 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
        assertNotNull(state);
        jobService.killJob(applicationId);
        state = getState(applicationId);
        assertNotNull(state);
        assertTrue(state.equals(YarnApplicationState.KILLED));

        ApplicationReport app = jobService.getJobReportById(applicationId);
        
        List<ApplicationReport> reports = jobService.getJobReportByUser(app.getUser());
        int numJobs = reports.size();
        assertTrue(numJobs > 0);

        applicationId = jobService.submitYarnJob("defaultYarnClient", appMasterProperties, containerProperties);

        state = waitState(applicationId, 10, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
        reports = jobService.getJobReportByUser(app.getUser());
        assertTrue(reports.size() > numJobs);
        jobService.killJob(applicationId);
    }

    @Test(groups = "functional", enabled = true)
    public void testCheckJobName() throws Exception {        
        Properties appMasterProperties = createAppMasterPropertiesForYarnJob();
        Properties containerProperties = createContainerPropertiesForYarnJob();
        
        ApplicationId applicationId = jobService.submitYarnJob("defaultYarnClient", appMasterProperties,
                containerProperties);
        waitState(applicationId, 10, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
        jobService.killJob(applicationId);

        ApplicationReport app = jobService.getJobReportById(applicationId);
        assertEquals(appMasterProperties.getProperty(AppMasterProperty.CUSTOMER.name()), 
                jobNameService.getCustomerFromJobName(app.getName()));
        assertTrue(jobNameService.getDateTimeFromJobName(app.getName()).isBeforeNow());
    }    
    
    @Test(groups = "functional", enabled = true)
    public void testSubmitPythonYarnJob() throws Exception {
        Classifier classifier = new Classifier();
        classifier.setName("IrisClassifier");
        classifier.setFeatures(Arrays.<String> asList(new String[] { "sepal_length", "sepal_width", "petal_length",
                "petal_width" }));
        classifier.setTargets(Arrays.<String> asList(new String[] { "category" }));
        classifier.setSchemaHdfsPath("/datascientist1/iris.json");
        classifier.setModelHdfsDir("/datascientist1/result");
        classifier.setPythonScriptHdfsPath("/datascientist1/nn_train.py");
        classifier.setTrainingDataHdfsPath("/training/nn_train.dat");
        classifier.setTestDataHdfsPath("/test/nn_test.dat");
        classifier.setDataFormat("csv");

        Properties appMasterProperties = createAppMasterPropertiesForYarnJob();

        Properties containerProperties = createContainerPropertiesForYarnJob();
        containerProperties.put(ContainerProperty.METADATA.name(), classifier.toString());

        ApplicationId applicationId = jobService
                .submitYarnJob("pythonClient", appMasterProperties, containerProperties);
        YarnApplicationState state = waitState(applicationId, 30, TimeUnit.SECONDS, YarnApplicationState.RUNNING);
        assertNotNull(state);
        ApplicationReport app = jobService.getJobReportById(applicationId);
        assertNotNull(app);
        state = waitState(applicationId, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
        assertEquals(state, YarnApplicationState.FINISHED);

        NumberFormat appIdFormat = getAppIdFormat();
        String jobId = applicationId.getClusterTimestamp() + "_" + appIdFormat.format(applicationId.getId());
        String modelFile = HdfsUtils.getFilesForDir(yarnConfiguration, "/datascientist1/result/" + jobId).get(0);
        String modelContents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelFile);
        assertEquals(modelContents.trim(), "this is the generated model.");

        String contextFileName = containerProperties.getProperty(ContainerProperty.APPMASTER_CONTEXT_FILE.name());
        String metadataFileName = containerProperties.getProperty(PythonContainerProperty.METADATA.name());

        assertFalse(new File(contextFileName).exists());
        assertFalse(new File(metadataFileName).exists());
    }

    @Test(groups = "functional", enabled = true)
    public void testSubmitMRJob() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(EventDataSamplingProperty.QUEUE.name(), "Priority0.MapReduce.0");
        properties.setProperty(EventDataSamplingProperty.INPUT.name(), inputDir);
        properties.setProperty(EventDataSamplingProperty.OUTPUT.name(), outputDir);
        properties.setProperty(EventDataSamplingProperty.SAMPLE_CONFIG.name(), samplingConfig.toString());
        properties.setProperty(EventDataSamplingProperty.CUSTOMER.name(), "Dell");
        ApplicationId applicationId = jobService.submitMRJob("samplingJob", properties);
        YarnApplicationState state = waitState(applicationId, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);

        state = getState(applicationId);
        assertNotNull(state);
        assertTrue(state.equals(YarnApplicationState.FINISHED));
        
        List<String> files = HdfsUtils.getFilesForDir(hadoopConfiguration, outputDir,
                new HdfsFilenameFilter() {

                    @Override
                    public boolean accept(Path filename) {
                        return filename.toString().endsWith(".avro");
                    }
            
        });
        assertEquals(4, files.size());
    }

    /**
     * This test needs to have SQOOP_HOME set and the $HADOOP_HOME/etc/hadoop part of the classpath.
     * src/test/resources/com/latticeengines/dataplatform/service/impl/mysql/create.sql should have been run
     *   before executing this test.
     * @throws Exception
     */
    @Test(groups = "functional", enabled = true)
    public void testLoadData() throws Exception {
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host("localhost").port(3306).db("dataplatformtest").user("root").password("welcome");
        DbCreds creds = new DbCreds(builder);
        ApplicationId appId = jobService.loadData("iris", "/tmp/import", creds, "Priority0.MapReduce.0", "Dell", 
        		Arrays.<String>asList(new String[] { "ID" }));
        YarnApplicationState state = waitState(appId, 120, TimeUnit.SECONDS, YarnApplicationState.FINISHED);
        assertEquals(state, YarnApplicationState.FINISHED);
        List<String> files = HdfsUtils.getFilesForDir(hadoopConfiguration, "/tmp/import",
                new HdfsFilenameFilter() {

                    @Override
                    public boolean accept(Path filename) {
                        return filename.toString().endsWith(".avro");
                    }
            
        });
        assertEquals(4, files.size());
    }
}
