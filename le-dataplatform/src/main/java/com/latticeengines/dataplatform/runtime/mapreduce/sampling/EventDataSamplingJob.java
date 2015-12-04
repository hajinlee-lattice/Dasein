package com.latticeengines.dataplatform.runtime.mapreduce.sampling;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MRJobCustomization;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.dataplatform.exposed.mapreduce.MRJobUtil;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;

public class EventDataSamplingJob extends Configured implements Tool, MRJobCustomization {

    public static final String LEDP_SAMPLE_CONFIG = "ledp.sample.config";
    private static final String SAMPLE_JOB_TYPE = "samplingJob";

    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    public EventDataSamplingJob(Configuration config) {
        setConf(config);
    }

    public EventDataSamplingJob(Configuration config, MapReduceCustomizationRegistry mapReduceCustomizationRegistry) {
        setConf(config);
        this.mapReduceCustomizationRegistry = mapReduceCustomizationRegistry;
        this.mapReduceCustomizationRegistry.register(this);
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

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new EventDataSamplingJob(new Configuration()), args);
        System.exit(res);
    }

    @Override
    public void customize(Job mrJob, Properties properties) {
        try {
            Configuration config = mrJob.getConfiguration();
            String samplingConfigStr = properties.getProperty(EventDataSamplingProperty.SAMPLE_CONFIG.name());
            config.set(LEDP_SAMPLE_CONFIG, samplingConfigStr);
            String queueName = properties.getProperty(MapReduceProperty.QUEUE.name());
            config.set("mapreduce.job.queuename", queueName);
            String inputDir = properties.getProperty(MapReduceProperty.INPUT.name());
            AvroKeyInputFormat.setInputPathFilter(mrJob, IgnoreDirectoriesAndSupportOnlyAvroFilesFilter.class);
            AvroKeyInputFormat.addInputPath(mrJob, new Path(inputDir));
            AvroKeyOutputFormat.setOutputPath(mrJob, new Path(properties.getProperty(MapReduceProperty.OUTPUT.name())));

            List<String> files = HdfsUtils.getFilesForDir(mrJob.getConfiguration(), inputDir, new HdfsFilenameFilter() {

                @Override
                public boolean accept(String filename) {
                    return filename.endsWith(".avro");
                }

            });
            String filename = files.size() > 0 ? files.get(0) : null;
            if (filename == null) {
                throw new LedpException(LedpCode.LEDP_12003, new String[] { inputDir });
            }
            Path path = new Path(filename);
            Schema schema = AvroUtils.getSchema(config, path);

            AvroJob.setInputKeySchema(mrJob, schema);
            AvroJob.setMapOutputValueSchema(mrJob, schema);
            AvroJob.setOutputKeySchema(mrJob, schema);
            AvroKeyOutputFormat.setCompressOutput(mrJob,
                    Boolean.valueOf(properties.getProperty(EventDataSamplingProperty.COMPRESS_SAMPLE.name(), "true")));

            SamplingConfiguration samplingConfig = JsonUtils
                    .deserialize(samplingConfigStr, SamplingConfiguration.class);

            for (SamplingElement samplingElement : samplingConfig.getSamplingElements()) {
                AvroMultipleOutputs.addNamedOutput(mrJob, samplingElement.getName() + "Training",
                        AvroKeyOutputFormat.class, schema);
                AvroMultipleOutputs.addNamedOutput(mrJob, samplingElement.getName() + "Test", AvroKeyOutputFormat.class,
                        schema);
            }

            mrJob.setInputFormatClass(AvroKeyInputFormat.class);
            mrJob.setMapOutputKeyClass(Text.class);
            mrJob.setMapOutputValueClass(AvroValue.class);
            mrJob.setMapperClass(EventDataSamplingMapper.class);
            mrJob.setReducerClass(EventDataSamplingReducer.class);
            mrJob.setOutputKeyClass(AvroKey.class);
            mrJob.setOutputValueClass(NullWritable.class);

            MRJobUtil.setLocalizedResources(mrJob, properties);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

    @Override
    public String getJobType() {
        return SAMPLE_JOB_TYPE;
    }

    static class IgnoreDirectoriesAndSupportOnlyAvroFilesFilter extends Configured implements PathFilter {
        private FileSystem fs;

        public IgnoreDirectoriesAndSupportOnlyAvroFilesFilter() {
            super();
        }

        public IgnoreDirectoriesAndSupportOnlyAvroFilesFilter(Configuration config) {
            super(config);
        }

        @Override
        public boolean accept(Path path) {
            try {

                if (this.getConf().get(FileInputFormat.INPUT_DIR).contains(path.toString())) {
                    return true;
                }
                if (!fs.isDirectory(path) && path.toString().endsWith(".avro")) {
                    return true;
                }
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_00002, e);
            }
            return false;
        }

        @Override
        public void setConf(Configuration config) {
            try {
                if (config != null) {
                    fs = FileSystem.get(config);
                    super.setConf(config);
                }

            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_00002, e);
            }
        }
    }

}
