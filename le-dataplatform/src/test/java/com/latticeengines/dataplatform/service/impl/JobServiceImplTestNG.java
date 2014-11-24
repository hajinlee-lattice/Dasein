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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.AfterClass;
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
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;

public class JobServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private ModelingJobService modelingJobService;

    @Autowired
    private Configuration hadoopConfiguration;

    @Autowired
    private JobNameService jobNameService;

    private String inputDir = null;
    private String outputDir = null;
    private SamplingConfiguration samplingConfig = null;
    private String baseDir = "/functionalTests/" + suffix;

    @BeforeClass(groups = {"functional", "functional.production"})
    public void setupSamplingMRJob() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);

        inputDir = ClassLoader.getSystemResource("com/latticeengines/dataplatform/runtime/mapreduce/DELL_EVENT_TABLE")
                .getPath();
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
            copyEntries.add(new CopyEntry("file:" + avroFile.getAbsolutePath(), baseDir + "/eventTable", false));
        }

        inputDir = baseDir + "/eventTable";
        outputDir = inputDir + "/samples";
        doCopy(fs, copyEntries);
    }

    @BeforeClass(groups = {"functional", "functional.production"})
    public void setup() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);

        fs.mkdirs(new Path(baseDir + "/training"));
        fs.mkdirs(new Path(baseDir + "/test"));
        fs.mkdirs(new Path(baseDir + "/datascientist1"));

        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();

        URL modelUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/service/impl/model.txt");

        String trainingFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_train.dat");
        String testFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_test.dat");
        String jsonFilePath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/iris.json");
        String pythonScriptPath = getFileUrlFromResource("com/latticeengines/dataplatform/service/impl/nn_train.py");
        FileUtils.copyFileToDirectory(new File(modelUrl.getFile()), new File("/tmp"));
        copyEntries.add(new CopyEntry(trainingFilePath, baseDir + "/training", false));
        copyEntries.add(new CopyEntry(testFilePath, baseDir + "/test", false));
        copyEntries.add(new CopyEntry(jsonFilePath, baseDir + "/datascientist1", false));
        copyEntries.add(new CopyEntry(pythonScriptPath, baseDir + "/datascientist1", false));
        doCopy(fs, copyEntries);
    }
    
    @AfterClass(groups = {"functional", "functional.production"})
    public void tearDown() throws Exception {
        FileSystem fs = FileSystem.get(yarnConfiguration);
        fs.delete(new Path("/functionalTests"), true);        
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

    @Test(groups = {"functional", "functional.production"}, enabled = true)
    public void testGetJobReportsAll() throws Exception {
        List<ApplicationReport> applications = modelingJobService.getJobReportsAll();
        assertNotNull(applications);
    }

    @Test(groups = {"functional", "functional.production"}, enabled = true)
    public void testKillApplication() throws Exception {
        Properties appMasterProperties = createAppMasterPropertiesForYarnJob();

        Properties containerProperties = createContainerPropertiesForYarnJob();

        ApplicationId applicationId = modelingJobService.submitYarnJob("defaultYarnClient", appMasterProperties,
                containerProperties);
        FinalApplicationStatus status = waitForStatus(applicationId, FinalApplicationStatus.UNDEFINED);
        assertEquals(status, FinalApplicationStatus.UNDEFINED);
        modelingJobService.killJob(applicationId);
        status = waitForStatus(applicationId, FinalApplicationStatus.KILLED);
        assertEquals(status, FinalApplicationStatus.KILLED);
    }

    @Test(groups = {"functional", "functional.production"}, enabled = true)
    public void testGetJobReportByUser() throws Exception {
        Properties appMasterProperties = createAppMasterPropertiesForYarnJob();

        Properties containerProperties = createContainerPropertiesForYarnJob();

        ApplicationId applicationId = modelingJobService.submitYarnJob("defaultYarnClient", appMasterProperties,
                containerProperties);
        FinalApplicationStatus status = waitForStatus(applicationId, FinalApplicationStatus.UNDEFINED);
        assertEquals(status, FinalApplicationStatus.UNDEFINED);
        modelingJobService.killJob(applicationId);
        status = waitForStatus(applicationId, FinalApplicationStatus.KILLED);
        assertNotNull(status);
        assertTrue(status.equals(FinalApplicationStatus.KILLED));

        ApplicationReport app = modelingJobService.getJobReportById(applicationId);

        List<ApplicationReport> reports = modelingJobService.getJobReportByUser(app.getUser());
        int numJobs = reports.size();
        assertTrue(numJobs > 0);

        applicationId = modelingJobService.submitYarnJob("defaultYarnClient", appMasterProperties, containerProperties);

        status = waitForStatus(applicationId, FinalApplicationStatus.UNDEFINED);
        assertEquals(status, FinalApplicationStatus.UNDEFINED);
        reports = modelingJobService.getJobReportByUser(app.getUser());
        assertTrue(reports.size() > numJobs);
        modelingJobService.killJob(applicationId);
    }

    @Test(groups = {"functional", "functional.production"}, enabled = true)
    public void testCheckJobName() throws Exception {
        Properties appMasterProperties = createAppMasterPropertiesForYarnJob();
        Properties containerProperties = createContainerPropertiesForYarnJob();

        ApplicationId applicationId = modelingJobService.submitYarnJob("defaultYarnClient", appMasterProperties,
                containerProperties);
        FinalApplicationStatus status = waitForStatus(applicationId, FinalApplicationStatus.UNDEFINED);
        assertEquals(status, FinalApplicationStatus.UNDEFINED);
        modelingJobService.killJob(applicationId);

        ApplicationReport app = modelingJobService.getJobReportById(applicationId);
        assertEquals(appMasterProperties.getProperty(AppMasterProperty.CUSTOMER.name()),
                jobNameService.getCustomerFromJobName(app.getName()));
        assertTrue(jobNameService.getDateTimeFromJobName(app.getName()).isBeforeNow());
    }

    @Test(groups = {"functional", "functional.production"}, enabled = true)
    public void testSubmitPythonYarnJob() throws Exception {
        Classifier classifier = new Classifier();
        classifier.setName("IrisClassifier");
        classifier.setFeatures(Arrays.<String> asList(new String[] { "sepal_length", "sepal_width", "petal_length",
                "petal_width" }));
        classifier.setTargets(Arrays.<String> asList(new String[] { "category" }));
        classifier.setSchemaHdfsPath(baseDir + "/datascientist1/iris.json");
        classifier.setModelHdfsDir(baseDir + "/datascientist1/result");
        classifier.setPythonScriptHdfsPath(baseDir + "/datascientist1/nn_train.py");
        classifier.setTrainingDataHdfsPath(baseDir + "/training/nn_train.dat");
        classifier.setTestDataHdfsPath(baseDir + "/test/nn_test.dat");
        classifier.setDataFormat("csv");
        classifier.setDataProfileHdfsPath(baseDir + "/datascientist1/EventMetadata");
        classifier.setConfigMetadataHdfsPath(baseDir + "/datascientist1/EventMetadata");
        classifier.setPythonPipelineLibHdfsPath("/app/dataplatform/scripts/lepipeline.tar.gz");
        classifier.setPythonPipelineScriptHdfsPath("/app/dataplatform/scripts/pipeline.py");

        Properties appMasterProperties = createAppMasterPropertiesForYarnJob();

        Properties containerProperties = createContainerPropertiesForYarnJob();
        containerProperties.put(ContainerProperty.METADATA.name(), classifier.toString());

        ApplicationId applicationId = modelingJobService.submitYarnJob("pythonClient", appMasterProperties,
                containerProperties);
        FinalApplicationStatus status = waitForStatus(applicationId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        NumberFormat appIdFormat = getAppIdFormat();
        String jobId = applicationId.getClusterTimestamp() + "_" + appIdFormat.format(applicationId.getId());
        String modelFile = HdfsUtils.getFilesForDir(yarnConfiguration, baseDir + "/datascientist1/result/" + jobId).get(0);
        String modelContents = HdfsUtils.getHdfsFileContents(yarnConfiguration, modelFile);
        assertEquals(modelContents.trim(), "this is the generated model.");

        String contextFileName = containerProperties.getProperty(ContainerProperty.APPMASTER_CONTEXT_FILE.name());
        String metadataFileName = containerProperties.getProperty(PythonContainerProperty.METADATA.name());

        assertFalse(new File(contextFileName).exists());
        assertFalse(new File(metadataFileName).exists());
    }

    @Test(groups = {"functional", "functional.production"}, enabled = true)
    public void testSubmitMRJob() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(EventDataSamplingProperty.QUEUE.name(), "Priority0.MapReduce.0");
        properties.setProperty(EventDataSamplingProperty.INPUT.name(), inputDir);
        properties.setProperty(EventDataSamplingProperty.OUTPUT.name(), outputDir);
        properties.setProperty(EventDataSamplingProperty.SAMPLE_CONFIG.name(), samplingConfig.toString());
        properties.setProperty(EventDataSamplingProperty.CUSTOMER.name(), "Dell");
        ApplicationId applicationId = modelingJobService.submitMRJob("samplingJob", properties);
        FinalApplicationStatus status = waitForStatus(applicationId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        List<String> files = HdfsUtils.getFilesForDir(hadoopConfiguration, outputDir, new HdfsFilenameFilter() {

            @Override
            public boolean accept(String filename) {
                return filename.endsWith(".avro");
            }

        });
        assertEquals(4, files.size());
    }

    /**
     * This test needs to have SQOOP_HOME set and the $HADOOP_HOME/etc/hadoop
     * part of the classpath.
     * src/test/resources/com/latticeengines/dataplatform/
     * service/impl/mysql/create.sql should have been run before executing this
     * test.
     *
     * @throws Exception
     */
    @Test(groups = {"functional", "functional.production"}, enabled = true)
    public void testLoadData() throws Exception {
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dataSourceHost).port(dataSourcePort).db(dataSourceDB).user(dataSourceUser)
                .password(dataSourcePasswd).type(dataSourceDBType);
        DbCreds creds = new DbCreds(builder);
        ApplicationId appId = modelingJobService.loadData("iris", baseDir + "/tmp/import", creds, "Priority0.MapReduce.0",
                "Dell", Arrays.<String> asList(new String[] { "ID" }));
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        List<String> files = HdfsUtils.getFilesForDir(hadoopConfiguration, baseDir + "/tmp/import", new HdfsFilenameFilter() {

            @Override
            public boolean accept(String filename) {
                return filename.endsWith(".avro");
            }

        });
        assertTrue(files.size() >= 1);
    }
}
