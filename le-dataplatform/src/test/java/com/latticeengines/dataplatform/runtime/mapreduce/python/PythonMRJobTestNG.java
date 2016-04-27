package com.latticeengines.dataplatform.runtime.mapreduce.python;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.annotation.Resource;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.yarn.fs.PrototypeLocalResourcesFactoryBean.CopyEntry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.mapreduce.MRJobUtil;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.runtime.mapreduce.python.aggregator.FileAggregator;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.dataplatform.runtime.python.PythonMRJobType;
import com.latticeengines.dataplatform.runtime.python.PythonMRProperty;
import com.latticeengines.dataplatform.service.modeling.ModelingJobService;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Transactional
public class PythonMRJobTestNG extends DataPlatformFunctionalTestNGBase {

    private static final int NUM_MAPPER = 4;

    @Resource(name = "parallelModelingJobService")
    ModelingJobService modelingJobService;

    @Value("${dataplatform.fs.web.defaultFS}")
    private String webFS;

    @Value("${dataplatform.debug:false}")
    private String debug;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    @Autowired
    private VersionManager versionManager;

    private String customer = "Nutanix";
    private String localDir = "com/latticeengines/dataplatform/runtime/mapreduce/Q_EVENT_NUTANIX";

    private String dataDir;
    private String metadataDir;
    private String sampleDir;
    private String profileDir;
    private String modelDir;

    private Classifier classifier;
    private String trainingSet = "TrainingAll-r-00000.avro";

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        String baseDir = customerBaseDir + "/" + customer;
        dataDir = baseDir + "/data/Q_EventTable_Nutanix";
        metadataDir = baseDir + "/data/EventMetadata";
        sampleDir = dataDir + "/samples";
        profileDir = metadataDir + "/profiles";
        modelDir = baseDir + "/models/Q_EventTable_Nutanix/58e6de15-5448-4009-a512-bd27d59ca75d";

        FileSystem fs = FileSystem.get(yarnConfiguration);
        if (fs.exists(new Path(baseDir))) {
            fs.delete(new Path(baseDir), true);
        }

        fs.mkdirs(new Path(dataDir));
        fs.mkdirs(new Path(metadataDir));
        fs.mkdirs(new Path(sampleDir));
        fs.mkdirs(new Path(profileDir));
        fs.mkdirs(new Path(modelDir));

        List<CopyEntry> copyEntries = copyFromLocalFiles();
        doCopy(fs, copyEntries);

    }

    private List<CopyEntry> copyFromLocalFiles() {
        List<CopyEntry> copyEntries = new ArrayList<CopyEntry>();
        String localPath = ClassLoader.getSystemResource(localDir).getPath();

        copyEntries.add(new CopyEntry("file:" + localPath + "/Q_EventTable_Nutanix.avsc", dataDir, false));
        copyEntries.add(new CopyEntry("file:" + localPath + "/metadata.avsc", metadataDir, false));

        File[] avroFiles = getAvroFilesForDir(localPath);
        for (File avroFile : avroFiles) {
            copyEntries.add(new CopyEntry("file:" + avroFile.getAbsolutePath(), sampleDir, false));
        }
        
        return copyEntries;
    }

    @Test(groups = { "functional" }, enabled = true)
    public void testProfiling() throws Exception {
        classifier = PythonMRTestUtils.readClassifier(localDir, "metadata-profile.json");
        setVersion(classifier, versionManager.getCurrentVersionInStack(stackName));
        int linesPerMap = createProfilingInputConfig(classifier.getFeatures(), profileDir);
        Properties properties = setupProperties(linesPerMap, profileDir, metadataDir,
                PythonMRJobType.PROFILING_JOB.jobType());

        ApplicationId appId = modelingJobService.submitMRJob(PythonMRJob.PYTHON_MR_JOB, properties);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, metadataDir + "/" + FileAggregator.PROFILE_AVRO));
        assertTrue(HdfsUtils.fileExists(yarnConfiguration, metadataDir + "/" + FileAggregator.DIAGNOSTICS_JSON));
    }

    private void setVersion(Classifier classifier, String currentVersion) {
        classifier.setPythonScriptHdfsPath(classifier.getPythonScriptHdfsPath().replaceAll("\\$\\$" + PythonContainerProperty.VERSION.name() + "\\$\\$", currentVersion));
        classifier.setPythonPipelineScriptHdfsPath(classifier.getPythonPipelineScriptHdfsPath().replaceAll("\\$\\$" + PythonContainerProperty.VERSION.name() + "\\$\\$", currentVersion));
        classifier.setPythonPipelineLibHdfsPath(classifier.getPythonPipelineLibHdfsPath().replaceAll("\\$\\$" + PythonContainerProperty.VERSION.name() + "\\$\\$", currentVersion));
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "testProfiling" })
    public void testModeling() throws Exception {
        classifier = PythonMRTestUtils.readClassifier(localDir, "metadata-learn.json");
        setVersion(classifier, versionManager.getCurrentVersionInStack(stackName));
        String modelInputDir = modelDir + "/modelName";
        int linesPerMap = createModelingInputConfig(sampleDir, modelInputDir);
        Properties property = setupProperties(linesPerMap, modelInputDir, modelDir,
                PythonMRJobType.MODELING_JOB.jobType());

        ApplicationId appId = modelingJobService.submitMRJob(PythonMRJob.PYTHON_MR_JOB, property);
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    private Properties setupProperties(int linesPerMap, String hdfsInDir, String hdfsOutDir, String jobType) {
        Properties properties = new Properties();

        String cacheFilePath = null;
        if (jobType == PythonMRJobType.PROFILING_JOB.jobType()) {
            cacheFilePath = PythonMRUtils.setupProfilingCacheFiles(classifier, 
                    MRJobUtil.getPlatformShadedJarPath(yarnConfiguration, versionManager.getCurrentVersionInStack(stackName)), versionManager.getCurrentVersionInStack(stackName));
        } else {
            List<String> trainingSets = new ArrayList<String>();
            trainingSets.add(trainingSet);
            cacheFilePath = PythonMRUtils.setupModelingCacheFiles(classifier, trainingSets,
                    MRJobUtil.getPlatformShadedJarPath(yarnConfiguration, versionManager.getCurrentVersionInStack(stackName)), versionManager.getCurrentVersionInStack(stackName));
        }

        String cacheArchivePath = PythonMRUtils.setupArchiveFilePath(classifier, versionManager.getCurrentVersionInStack(stackName));
        String[] tokens = classifier.getPythonPipelineLibHdfsPath().split("/");

        properties.put(MapReduceProperty.INPUT.name(), hdfsInDir);
        properties.put(PythonMRProperty.LINES_PER_MAP.name(), String.valueOf(linesPerMap));

        properties.put(MapReduceProperty.OUTPUT.name(), hdfsOutDir);
        properties.put(MapReduceProperty.CUSTOMER.name(), customer);
        String assignedQueue = LedpQueueAssigner.getModelingQueueNameForSubmission();
        properties.setProperty(MapReduceProperty.QUEUE.name(), assignedQueue);
        properties.put(MapReduceProperty.JOB_TYPE.name(), jobType);
        properties.put(MapReduceProperty.CACHE_FILE_PATH.name(), cacheFilePath);
        properties.put(MapReduceProperty.CACHE_ARCHIVE_PATH.name(), cacheArchivePath);

        properties.put(PythonMRProperty.PYTHONPATH.name(), ".:leframework.tar.gz:" + tokens[tokens.length - 1]);
        properties.put(PythonMRProperty.PYTHONIOENCODING.name(), "UTF-8");
        properties.put(PythonMRProperty.SHDP_HD_FSWEB.name(), webFS);
        properties.put(PythonMRProperty.DEBUG.name(), debug);
        properties.put(PythonContainerProperty.METADATA_CONTENTS.name(), classifier.toString());

        return properties;
    }

    private int createProfilingInputConfig(List<String> features, String hdfsDir) {
        int size = features.size();
        if (size < NUM_MAPPER) {
            throw new RuntimeException("Feature size less than the number of mapper");
        }

        int linesPerMap = size / NUM_MAPPER;
        String profileConfig = PythonMRJobType.PROFILING_JOB.configName();

        try {
            FileUtils.writeLines(new File(profileConfig), features);
            HdfsUtils.copyLocalToHdfs(yarnConfiguration, profileConfig, hdfsDir);
            FileUtils.deleteQuietly(new File(profileConfig));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return linesPerMap;
    }

    private int createModelingInputConfig(String sampleDir, String modelDir) {
        int linesPerMap = 1;
        String modelConfig = PythonMRJobType.MODELING_JOB.configName();

        try {
            List<String> content = new ArrayList<String>();
            for (int i = 0; i < NUM_MAPPER; i++) {
                content.add(trainingSet);
            }
            HdfsUtils.writeToFile(yarnConfiguration, modelDir + "/" + modelConfig,
                    StringUtils.join(content, System.lineSeparator()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return linesPerMap;
    }
}
