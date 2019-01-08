package com.latticeengines.dataplatform.service.impl.modeling;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFormat;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.runtime.mapreduce.python.PythonMRJob;
import com.latticeengines.dataplatform.runtime.mapreduce.python.PythonMRUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.Classifier;
import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.algorithm.RandomForestAlgorithm;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.mapreduce.MRJobUtil;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.yarn.exposed.runtime.python.PythonContainerProperty;
import com.latticeengines.yarn.exposed.runtime.python.PythonMRJobType;
import com.latticeengines.yarn.exposed.runtime.python.PythonMRProperty;

@Component("parallelModelingJobService")
public class ParallelModelingJobServiceImpl extends ModelingJobServiceImpl {

    @Value("${hadoop.fs.web.defaultFS}")
    private String webFS;

    @Value("${dataplatform.container.parallel.map.memory}")
    private String mapMemorySize;

    @Value("${dataplatform.container.parallel.reduce.memory}")
    private String reduceMemorySize;

    @Value("${dataplatform.container.mapreduce.memory}")
    private String profilingMapReduceMemorySize;

    @Value("${dataplatform.debug:false}")
    private String debug;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    @Value("${dataplatform.yarn.job.runtime.config}")
    private String runtimeConfig;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Inject
    private VersionManager versionManager;

    @Inject
    private EMRCacheService emrCacheService;

    private void setDefaultValues(Classifier classifier) {
        RandomForestAlgorithm rf = new RandomForestAlgorithm();
        rf.resetAlgorithmProperties();
        if (StringUtils.isEmpty(classifier.getPipelineDriver())) {
            classifier.setPipelineDriver(rf.getPipelineDriver());
        }
    }

    protected ApplicationId submitJobInternal(ModelingJob modelingJob) {
        Properties properties = configJobInternal(modelingJob);
        return super.submitMRJob(PythonMRJob.PYTHON_MR_JOB, properties);
    }

    protected Properties configJobInternal(ModelingJob modelingJob) {
        Properties appMasterProperties = modelingJob.getAppMasterPropertiesObject();
        Properties containerProperties = modelingJob.getContainerPropertiesObject();

        String metadata = containerProperties.getProperty(ContainerProperty.METADATA.name());
        containerProperties.put(PythonContainerProperty.METADATA_CONTENTS.name(), metadata);
        Classifier classifier = JsonUtils.deserialize(metadata, Classifier.class);
        setDefaultValues(classifier);

        String jobType = containerProperties.getProperty(ContainerProperty.JOB_TYPE.name());
        String cacheArchivePath = PythonMRUtils.setupArchiveFilePath(classifier,
                versionManager.getCurrentVersionInStack(stackName));

        Properties properties = new Properties();
        String inputDir = classifier.getModelHdfsDir() + "/" + classifier.getName();
        int mapperSize = Integer.parseInt(appMasterProperties.getProperty(PythonMRProperty.MAPPER_SIZE.name()));
        properties.put(MapReduceProperty.CACHE_FILE_PATH.name(), MRJobUtil.getPlatformShadedJarPath(yarnConfiguration,
                versionManager.getCurrentVersionInStack(stackName)));
        try {
            if (PythonMRJobType.PROFILING_JOB.jobType().equals(jobType)) {
                setupProfilingMRConfig(properties, classifier, mapperSize, inputDir);
            } else if (PythonMRJobType.MODELING_JOB.jobType().equals(jobType)) {
                setupModelingMRConfig(properties, classifier, mapperSize, inputDir);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15015, e);
        }

        properties.put(MapReduceProperty.CUSTOMER.name(),
                appMasterProperties.getProperty(AppMasterProperty.CUSTOMER.name()));
        properties.setProperty(MapReduceProperty.QUEUE.name(),
                appMasterProperties.getProperty(AppMasterProperty.QUEUE.name()));
        properties.put(MapReduceProperty.JOB_TYPE.name(), jobType);
        properties.put(MapReduceProperty.INPUT.name(), inputDir);
        properties.put(MapReduceProperty.OUTPUT.name(), classifier.getModelHdfsDir());
        properties.put(MapReduceProperty.CACHE_ARCHIVE_PATH.name(), cacheArchivePath);

        String pipelineLibFile = StringUtils.substringAfterLast(classifier.getPythonPipelineLibHdfsPath(), "/");
        properties.put(PythonMRProperty.PYTHONPATH.name(), ".:leframework.tar.gz:" + pipelineLibFile);
        properties.put(PythonMRProperty.PYTHONIOENCODING.name(), "UTF-8");
        if (Boolean.TRUE.equals(useEmr)) {
            properties.put(PythonMRProperty.SHDP_HD_FSWEB.name(), emrCacheService.getWebHdfsUrl());
        } else {
            properties.put(PythonMRProperty.SHDP_HD_FSWEB.name(), webFS);
        }
        properties.put(PythonMRProperty.DEBUG.name(), debug);
        properties.put(PythonContainerProperty.METADATA_CONTENTS.name(), classifier.toString());
        properties.put(PythonContainerProperty.RUNTIME_CONFIG.name(), runtimeConfig);
        return properties;
    }

    private void setupProfilingMRConfig(Properties properties, Classifier classifier, int mapperSize, String inputDir)
            throws Exception {
        List<String> features = classifier.getFeatures();
        int featureSize = features.size();
        if (featureSize < mapperSize) {
            mapperSize = featureSize;
        }
        String linesPerMap = String.valueOf((featureSize + mapperSize - 1) / mapperSize); // round
                                                                                          // up
        String cacheFilePath = PythonMRUtils.setupProfilingCacheFiles(classifier,
                properties.get(MapReduceProperty.CACHE_FILE_PATH.name()).toString(),
                versionManager.getCurrentVersionInStack(stackName));

        properties.put(PythonMRProperty.LINES_PER_MAP.name(), linesPerMap);
        properties.put(MapReduceProperty.CACHE_FILE_PATH.name(), cacheFilePath);

        setProfilingMapReduceMemory(properties);

        HdfsUtils.writeToFile(yarnConfiguration, inputDir + "/" + PythonMRJobType.PROFILING_JOB.configName(),
                StringUtils.join(features, System.lineSeparator()));
    }

    private void setupModelingMRConfig(Properties properties, Classifier classifier, int mapperSize, String inputDir)
            throws Exception {
        String linesPerMap = "1";
        List<String> trainingFiles = new ArrayList<String>();
        String trainingDir = StringUtils.substringBeforeLast(classifier.getTrainingDataHdfsPath(), "/");
        List<String> trainingPaths = HdfsUtils.getFilesForDir(yarnConfiguration, trainingDir,
                SamplingConfiguration.TRAINING_SET_PREFIX + HdfsFileFormat.AVRO_FILE);

        for (String path : trainingPaths) {
            trainingFiles.add(StringUtils.substringAfterLast(path, "/"));
        }
        String cacheFilePath = PythonMRUtils.setupModelingCacheFiles(classifier, trainingFiles,
                properties.get(MapReduceProperty.CACHE_FILE_PATH.name()).toString(),
                versionManager.getCurrentVersionInStack(stackName));

        properties.put(PythonMRProperty.LINES_PER_MAP.name(), linesPerMap);
        properties.put(MapReduceProperty.CACHE_FILE_PATH.name(), cacheFilePath);

        setModelingMapReduceMemory(properties);

        HdfsUtils.writeToFile(yarnConfiguration, inputDir + "/" + PythonMRJobType.MODELING_JOB.configName(),
                StringUtils.join(trainingFiles, System.lineSeparator()));
    }

    private void setProfilingMapReduceMemory(Properties properties) {
        properties.put(MapReduceProperty.MAP_MEMORY_SIZE.name(), profilingMapReduceMemorySize);
        properties.put(MapReduceProperty.REDUCE_MEMORY_SIZE.name(), profilingMapReduceMemorySize);
    }

    private void setModelingMapReduceMemory(Properties properties) {
        properties.put(MapReduceProperty.MAP_MEMORY_SIZE.name(), mapMemorySize);
        properties.put(MapReduceProperty.REDUCE_MEMORY_SIZE.name(), reduceMemorySize);
    }

}
