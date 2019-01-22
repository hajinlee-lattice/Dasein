package com.latticeengines.dataplatform.runtime.mapreduce.sampling.parallel;

import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFormat;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.runtime.mapreduce.MRPathFilter;
import com.latticeengines.dataplatform.runtime.mapreduce.sampling.EventDataSamplingProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.EventCounterConfiguration;
import com.latticeengines.yarn.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.yarn.exposed.mapreduce.MRJobUtil;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.yarn.exposed.runtime.mapreduce.MRJobCustomizationBase;

public class EventCounterJob extends MRJobCustomizationBase {

    public static final String LEDP_EVENT_COUNTER_CONFIG = "ledp.event.counter.config";
    private static final String EVENT_COUNTER_JOB_TYPE = "eventCounterJob";
    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    public EventCounterJob(Configuration config) {
        super(config);
    }

    public EventCounterJob(Configuration config, MapReduceCustomizationRegistry mapReduceCustomizationRegistry) {
        this(config);
        this.mapReduceCustomizationRegistry = mapReduceCustomizationRegistry;
        this.mapReduceCustomizationRegistry.register(this);
    }

    @Override
    public String getJobType() {
        return EVENT_COUNTER_JOB_TYPE;
    }

    @Override
    public void customize(Job mrJob, Properties properties) {
        Configuration config = mrJob.getConfiguration();
        customizeConfig(config, properties);

        MRJobUtil.setLocalizedResources(mrJob, properties);

        setInputFormat(mrJob, config, properties);
        setOutputFormat(mrJob, properties);

        Schema schema = getSchema(mrJob, config, properties);
        setSchema(mrJob, schema);

        customizeJob(mrJob);
        customizeSampling(mrJob, properties, schema);

    }

    private void customizeConfig(Configuration config, Properties properties) {
        String eventCounterConfigStr = properties.getProperty(EventDataSamplingProperty.SAMPLE_CONFIG.name());
        config.set(LEDP_EVENT_COUNTER_CONFIG, eventCounterConfigStr);
        String queueName = properties.getProperty(MapReduceProperty.QUEUE.name());
        config.set("mapreduce.job.queuename", queueName);

        config.set(MRPathFilter.INPUT_FILE_PATTERN, HdfsFileFormat.AVRO_FILE);
    }

    private void setInputFormat(Job job, Configuration config, Properties properties) {
        String inputDir = properties.getProperty(MapReduceProperty.INPUT.name());
        try {
            AvroKeyInputFormat.setInputPathFilter(job, MRPathFilter.class);
            AvroKeyInputFormat.addInputPath(job, new Path(inputDir));
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_15008, e);
        }

    }

    private void setOutputFormat(Job job, Properties properties) {
        job.setOutputFormatClass(NullOutputFormat.class);
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
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(AvroKey.class);
        job.setOutputValueClass(LongWritable.class);
    }

    private void customizeSampling(Job job, Properties properties, Schema schema) {
        String samplingConfigStr = properties.getProperty(EventDataSamplingProperty.SAMPLE_CONFIG.name());
        System.out.println("samplingConfigStr = " + samplingConfigStr);
        EventCounterConfiguration samplingConfig = JsonUtils.deserialize(samplingConfigStr,
                EventCounterConfiguration.class);
        customizeJob(job, samplingConfig);
    }

    public void customizeJob(Job job, EventCounterConfiguration samplingConfig) {
        customizeMapper(job, samplingConfig);
        customizePartitioner(job, samplingConfig);
        customizeReducer(job, samplingConfig);
    }

    protected void customizeMapper(Job job, EventCounterConfiguration samplingConfig) {
        job.setMapperClass(EventCountMapper.class);
    }

    protected void customizePartitioner(Job job, EventCounterConfiguration samplingConfig) {
    }

    protected void customizeReducer(Job job, EventCounterConfiguration samplingConfig) {
    }

    @SuppressWarnings({ "deprecation" })
    @Override
    public int run(String[] args) throws Exception {
        return 1;
    }
}
