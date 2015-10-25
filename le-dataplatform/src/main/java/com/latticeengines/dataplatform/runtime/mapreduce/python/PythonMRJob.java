package com.latticeengines.dataplatform.runtime.mapreduce.python;

import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.latticeengines.dataplatform.exposed.client.mapreduce.MRJobCustomization;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.runtime.mapreduce.MRPathFilter;
import com.latticeengines.dataplatform.runtime.python.PythonContainerProperty;
import com.latticeengines.dataplatform.runtime.python.PythonMRJobType;
import com.latticeengines.dataplatform.runtime.python.PythonMRProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class PythonMRJob extends Configured implements MRJobCustomization {
    public static final String PYTHON_MR_JOB = "pythonMRJob";

    private static final String comma = ",";
    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    public PythonMRJob(Configuration config) {
        setConf(config);
    }

    public PythonMRJob(Configuration config, MapReduceCustomizationRegistry mapReduceCustomizationRegistry) {
        setConf(config);
        this.mapReduceCustomizationRegistry = mapReduceCustomizationRegistry;
        this.mapReduceCustomizationRegistry.register(this);
    }

    @Override
    public String getJobType() {
        return PYTHON_MR_JOB;
    }

    @Override
    public void customize(Job mrJob, Properties properties) {
        Configuration config = mrJob.getConfiguration();
        customizeConfig(config, properties);
        setLocalizedResources(mrJob, properties);

        setInputFormat(mrJob, properties, config);
        mrJob.setOutputFormatClass(NullOutputFormat.class);

        String jobType = config.get(MapReduceProperty.JOB_TYPE.name());
        if (jobType == PythonMRJobType.MODELING_JOB.jobType()) {
            mrJob.setMapperClass(PythonModelingMapper.class);
            mrJob.setReducerClass(PythonReducer.class);
            mrJob.setNumReduceTasks(1);

        } else if (jobType == PythonMRJobType.PROFILING_JOB.jobType()) {
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
        String reduceMemorySize = properties.getProperty(MapReduceProperty.REDUCE_MEMORY_SIZE.name());
        if (reduceMemorySize != null) {
            config.set("mapreduce.reduce.memory.mb", reduceMemorySize);
        }
    }

    private void setInputFormat(Job mrJob, Properties properties, Configuration config) {
        try {
            int linesPerMap = Integer.parseInt(properties.getProperty(PythonMRProperty.LINES_PER_MAP.name()));
            NLineInputFormat.setNumLinesPerSplit(mrJob, linesPerMap);

            String inputDir = properties.getProperty(MapReduceProperty.INPUT.name());
            NLineInputFormat.addInputPath(mrJob, new Path(inputDir));

            NLineInputFormat.setInputPathFilter(mrJob, MRPathFilter.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15008, e);
        }
        mrJob.setInputFormatClass(NLineInputFormat.class);
    }

    private void setLocalizedResources(Job mrJob, Properties properties) {
        String cachPaths = properties.getProperty(MapReduceProperty.CACHE_FILE_PATH.name());
        String cacheArchivePaths = properties.getProperty(MapReduceProperty.CACHE_ARCHIVE_PATH.name());

        setCacheFiles(mrJob, cachPaths);
        setCacheArchiveFiles(mrJob, cacheArchivePaths);
    }

    private void setCacheFiles(Job mrJob, String cachePaths) {
        if (cachePaths == null)
            return;
        try {
            URI[] caches = getCommaSeparatedURI(cachePaths);
            mrJob.setCacheFiles(caches);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15009, e);
        }
    }

    private void setCacheArchiveFiles(Job mrJob, String cacheArchivePaths) {
        if (cacheArchivePaths == null)
            return;
        try {
            URI[] cacheArchives = getCommaSeparatedURI(cacheArchivePaths);
            mrJob.setCacheArchives(cacheArchives);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15010, e);
        }
    }

    private URI[] getCommaSeparatedURI(String paths) throws Exception {
        String[] files = paths.split(comma);
        URI[] filesURI = new URI[files.length];
        for (int i = 0; i < filesURI.length; i++) {
            filesURI[i] = new URI(files[i].trim());
        }
        return filesURI;
    }

}