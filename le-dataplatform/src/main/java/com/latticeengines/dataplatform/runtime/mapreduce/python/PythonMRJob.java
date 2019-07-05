package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.runtime.mapreduce.MRPathFilter;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.hadoop.exposed.service.ManifestService;
import com.latticeengines.yarn.exposed.client.mapreduce.MRJobCustomization;
import com.latticeengines.yarn.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.yarn.exposed.mapreduce.MRJobUtil;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.yarn.exposed.runtime.python.PythonContainerProperty;
import com.latticeengines.yarn.exposed.runtime.python.PythonMRJobType;
import com.latticeengines.yarn.exposed.runtime.python.PythonMRProperty;

public class PythonMRJob extends Configured implements MRJobCustomization {
    public static final String PYTHON_MR_JOB = "pythonMRJob";

    private VersionManager versionManager;
    private ManifestService manifestService;
    private EMRCacheService emrCacheService;
    private String stackName;
    private String condaEnv;
    private String condaEnvAmbari;
    private boolean useEmr;

    public PythonMRJob(Configuration config) {
        setConf(config);
        this.condaEnv = "lattice";
        this.condaEnvAmbari = "lattice";
        this.useEmr = false;
    }

    public PythonMRJob(//
            Configuration config, //
            MapReduceCustomizationRegistry mapReduceCustomizationRegistry, //
            ManifestService manifestService, //
            EMRCacheService emrCacheService, //
            String stackName, //
            String condaEnv, //
            String condaEnvAmbari, //
            Boolean useEmr) {
        setConf(config);
        mapReduceCustomizationRegistry.register(this);
        this.manifestService = manifestService;
        this.emrCacheService = emrCacheService;
        this.stackName = stackName;
        this.condaEnv = condaEnv;
        this.condaEnvAmbari = condaEnvAmbari;
        this.useEmr = Boolean.TRUE.equals(useEmr);
    }

    @VisibleForTesting
    void setVersionManager(VersionManager versionManager) {
        this.versionManager = versionManager;
    }

    @VisibleForTesting
    void setManifestService(ManifestService manifestService) {
        this.manifestService = manifestService;
    }

    @Override
    public String getJobType() {
        return PYTHON_MR_JOB;
    }

    @Override
    public void customize(Job mrJob, Properties properties) {
        Configuration config = mrJob.getConfiguration();
        customizeConfig(config, properties);
        MRJobUtil.setLocalizedResources(mrJob, properties);

        String opts = config.get(MRJobConfig.MAP_JAVA_OPTS, "");
        config.set(MRJobConfig.MAP_JAVA_OPTS, opts + " -Dlog4j.configurationFile=log4j2-yarn.xml" //
                        + " -DLOG4J_LE_LEVEL=INFO");

        setInputFormat(mrJob, properties, config);
        mrJob.setOutputFormatClass(NullOutputFormat.class);

        String jobType = config.get(MapReduceProperty.JOB_TYPE.name());
        if (PythonMRJobType.MODELING_JOB.jobType().equals(jobType)) {
            mrJob.setMapperClass(PythonModelingMapper.class);
            mrJob.setReducerClass(PythonReducer.class);
            mrJob.setNumReduceTasks(1);

        } else if (PythonMRJobType.PROFILING_JOB.jobType().equals(jobType)) {
            mrJob.setMapperClass(PythonProfilingMapper.class);
            mrJob.setReducerClass(PythonReducer.class);
            mrJob.setNumReduceTasks(2);
        }

    }

    private void customizeConfig(Configuration config, Properties properties) {
        String queueName = properties.getProperty(MapReduceProperty.QUEUE.name());
        config.set("mapreduce.job.queuename", queueName);

        String metadataKey = PythonContainerProperty.METADATA_CONTENTS.name();
        config.set(metadataKey, properties.getProperty(metadataKey));
        String runtimeConfigKey = PythonContainerProperty.RUNTIME_CONFIG.name();
        config.set(runtimeConfigKey, properties.getProperty(runtimeConfigKey));

        String jobTypeKey = MapReduceProperty.JOB_TYPE.name();
        config.set(jobTypeKey, properties.getProperty(jobTypeKey));

        String inputKey = MapReduceProperty.INPUT.name();
        config.set(inputKey, properties.getProperty(inputKey));
        String outputKey = MapReduceProperty.OUTPUT.name();
        config.set(outputKey, properties.getProperty(outputKey));
        config.set(MRPathFilter.INPUT_FILE_PATTERN, PythonMRJobType.CONFIG_FILE);

        String mapMemorySize = properties.getProperty(MapReduceProperty.MAP_MEMORY_SIZE.name());
        if (mapMemorySize != null) {
            config.set("mapreduce.map.memory.mb", mapMemorySize);
        }
        String reduceMemorySize = properties
                .getProperty(MapReduceProperty.REDUCE_MEMORY_SIZE.name());
        if (reduceMemorySize != null) {
            config.set("mapreduce.reduce.memory.mb", reduceMemorySize);
        }
        config.set(PythonContainerProperty.VERSION.name(), manifestService.getLedsVersion());

        config.set("mapreduce.job.maxtaskfailures.per.tracker", "1");
        config.set("mapreduce.map.maxattempts", "1");
        config.set("mapreduce.reduce.maxattempts", "1");
        config.set(PythonContainerProperty.CONDA_ENV.name(), getCondaEnv());
        if (Boolean.TRUE.equals(useEmr)) { // useEmr might be null
            config.set(PythonMRProperty.SHDP_HD_FSWEB.name(), emrCacheService.getWebHdfsUrl());
        }
    }

    private void setInputFormat(Job mrJob, Properties properties, Configuration config) {
        try {
            int linesPerMap = Integer
                    .parseInt(properties.getProperty(PythonMRProperty.LINES_PER_MAP.name()));
            NLineInputFormat.setNumLinesPerSplit(mrJob, linesPerMap);

            String inputDir = properties.getProperty(MapReduceProperty.INPUT.name());
            NLineInputFormat.addInputPath(mrJob, new Path(inputDir));

            NLineInputFormat.setInputPathFilter(mrJob, MRPathFilter.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15008, e);
        }
        mrJob.setInputFormatClass(NLineInputFormat.class);
    }

    private String getCondaEnv() {
        if (Boolean.TRUE.equals(useEmr)) {
            return condaEnv;
        } else {
            return condaEnvAmbari;
        }
    }

}
