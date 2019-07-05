package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel;

import java.util.List;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.ToolRunner;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFormat;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.runtime.mapreduce.MRPathFilter;
import com.latticeengines.dataplatform.runtime.mapreduce.sampling.EventDataSamplingProperty;
import com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.customizer.SamplingJobCustomizer;
import com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel.customizer.SamplingJobCustomizerFactory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.domain.exposed.modeling.SamplingType;
import com.latticeengines.yarn.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.yarn.exposed.mapreduce.MRJobUtil;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.yarn.exposed.runtime.mapreduce.MRJobCustomizationBase;

public class ParallelEventDataSamplingJob extends MRJobCustomizationBase {

    public static final String LEDP_SAMPLE_CONFIG = "ledp.sample.config";
    private static final String SAMPLE_JOB_TYPE = "parallelSamplingJob";

    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    @Inject
    private SamplingJobCustomizerFactory samplingJobCustomizerFactory;

    public ParallelEventDataSamplingJob(Configuration config) {
        super(config);
        samplingJobCustomizerFactory = new SamplingJobCustomizerFactory();
    }

    public ParallelEventDataSamplingJob(Configuration config,
            MapReduceCustomizationRegistry mapReduceCustomizationRegistry) {
        this(config);
        this.mapReduceCustomizationRegistry = mapReduceCustomizationRegistry;
        this.mapReduceCustomizationRegistry.register(this);
    }

    @Override
    public String getJobType() {
        return SAMPLE_JOB_TYPE;
    }

    @Override
    public void customize(Job mrJob, Properties properties) {
        Configuration config = mrJob.getConfiguration();
        customizeConfig(config, properties);

        MRJobUtil.setLocalizedResources(mrJob, properties);

        String opts = config.get(MRJobConfig.MAP_JAVA_OPTS, "");
        config.set(MRJobConfig.MAP_JAVA_OPTS, opts + " -Dlog4j.configurationFile=log4j2-yarn.xml" //
                + " -DLOG4J_LE_LEVEL=INFO");

        setInputFormat(mrJob, config, properties);
        setOutputFormat(mrJob, properties);

        Schema schema = getSchema(mrJob, config, properties);
        setSchema(mrJob, schema);

        customizeJob(mrJob);
        customizeSampling(mrJob, properties, schema);
    }

    private void customizeConfig(Configuration config, Properties properties) {
        String samplingConfigStr = properties.getProperty(EventDataSamplingProperty.SAMPLE_CONFIG.name());
        config.set(LEDP_SAMPLE_CONFIG, samplingConfigStr);
        String queueName = properties.getProperty(MapReduceProperty.QUEUE.name());
        config.set("mapreduce.job.queuename", queueName);

        config.set(MRPathFilter.INPUT_FILE_PATTERN, HdfsFileFormat.AVRO_FILE);
    }

    private void setInputFormat(Job job, Configuration config, Properties properties) {
        String inputDir = properties.getProperty(MapReduceProperty.INPUT.name());
        try {
            AvroKeyInputFormat.setInputPathFilter(job, MRPathFilter.class);
            AvroKeyInputFormat.addInputPath(job, new Path(inputDir));
            AvroKeyOutputFormat.setOutputPath(job, new Path(properties.getProperty(MapReduceProperty.OUTPUT.name())));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15008, e);
        }

    }

    private void setOutputFormat(Job job, Properties properties) {
        AvroKeyOutputFormat.setCompressOutput(job,
                Boolean.valueOf(properties.getProperty(EventDataSamplingProperty.COMPRESS_SAMPLE.name(), "true")));
    }

    private Schema getSchema(Job job, Configuration config, Properties properties) {
        String inputDir = properties.getProperty(MapReduceProperty.INPUT.name());

        List<String> files;
        try {
            files = HdfsUtils.getFilesForDir(job.getConfiguration(), inputDir, HdfsFileFormat.AVRO_FILE);
            if (CollectionUtils.isEmpty(files)) {
                throw new LedpException(LedpCode.LEDP_12003, new String[] { inputDir });
            }
            String filename = files.get(0);
            Path path = new Path(filename);
            Schema schema = AvroUtils.getSchema(config, path);
            return schema;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }

    }

    private void setSchema(Job job, Schema schema) {
        AvroJob.setInputKeySchema(job, schema);
        AvroJob.setMapOutputValueSchema(job, schema);
        AvroJob.setOutputKeySchema(job, schema);
    }

    private void customizeJob(Job job) {
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvroValue.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(NullWritable.class);
    }

    private void customizeSampling(Job job, Properties properties, Schema schema) {
        String samplingConfigStr = properties.getProperty(EventDataSamplingProperty.SAMPLE_CONFIG.name());
        SamplingConfiguration samplingConfig = JsonUtils.deserialize(samplingConfigStr, SamplingConfiguration.class);

        List<SamplingElement> samplingElements = samplingConfig.getSamplingElements();
        for (SamplingElement samplingElement : samplingElements) {
            AvroMultipleOutputs.addNamedOutput(job, samplingElement.getName(), AvroKeyOutputFormat.class, schema);
        }
        SamplingType samplingType = samplingConfig.getSamplingType();
        SamplingJobCustomizer samplingJobCustomizer = samplingJobCustomizerFactory.getCustomizer(samplingType);
        samplingJobCustomizer.customizeJob(job, samplingConfig);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ParallelEventDataSamplingJob(new Configuration()), args);
        System.exit(res);
    }

    @SuppressWarnings({ "deprecation" })
    @Override
    public int run(String[] args) throws Exception {
        JobConf jobConf = new JobConf(getConf(), getClass());

        Job job = new Job(jobConf);

        Properties properties = new Properties();
        properties.setProperty(MapReduceProperty.INPUT.name(), args[0]);
        properties.setProperty(MapReduceProperty.OUTPUT.name(), args[1]);
        properties.setProperty(EventDataSamplingProperty.SAMPLE_CONFIG.name(), args[2]);
        properties.setProperty(MapReduceProperty.QUEUE.name(), args[3]);

        customize(job, properties);
        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }
}
