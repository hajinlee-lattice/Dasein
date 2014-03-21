package com.latticeengines.dataplatform.runtime.mapreduce;

import java.io.File;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.FsInput;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.latticeengines.dataplatform.exposed.domain.SamplingConfiguration;
import com.latticeengines.dataplatform.exposed.domain.SamplingElement;
import com.latticeengines.dataplatform.exposed.exception.LedpCode;
import com.latticeengines.dataplatform.exposed.exception.LedpException;
import com.latticeengines.dataplatform.mapreduce.job.MRJobCustomization;
import com.latticeengines.dataplatform.util.HdfsHelper;
import com.latticeengines.dataplatform.util.JsonHelper;

public class EventDataSamplingJob extends Configured implements Tool, MRJobCustomization {
    
    public static String LEDP_SAMPLE_CONFIG = "ledp.sample.config";

    @SuppressWarnings({ "deprecation" })
    @Override
    public int run(String[] args) throws Exception {
        JobConf jobConf = new JobConf(getConf(), getClass());

        Job job = new Job(jobConf);

        Properties properties = new Properties();
        properties.setProperty(EventDataSamplingProperty.INPUT.name(), args[0]);
        properties.setProperty(EventDataSamplingProperty.OUTPUT.name(), args[1]);
        properties.setProperty(EventDataSamplingProperty.SAMPLE_CONFIG.name(), args[2]);

        customize(job, properties);
        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("/tmp/sample"));
        int res = ToolRunner.run(new EventDataSamplingJob(), args);
        System.exit(res);
    }

    @Override
    public void customize(Job job, Properties properties) {
        try {
            Configuration config = job.getConfiguration();
            String samplingConfigStr = properties.getProperty(EventDataSamplingProperty.SAMPLE_CONFIG.name());
            config.set(LEDP_SAMPLE_CONFIG, samplingConfigStr);
            String inputDir = properties.getProperty(EventDataSamplingProperty.INPUT.name());
            AvroKeyInputFormat.addInputPath(job, new Path(inputDir));
            AvroKeyOutputFormat.setOutputPath(job,
                    new Path(properties.getProperty(EventDataSamplingProperty.OUTPUT.name())));

            List<String> files = HdfsHelper.getFilesForDir(job.getConfiguration(), inputDir);
            String filename = null;
            for (String file : files) {
                if (file.endsWith(".avro")) {
                    filename = file;
                }
            }
            if (filename == null) {
                throw new LedpException(LedpCode.LEDP_12003, new String[] { inputDir });
            }
            Path path = new Path(filename);
            SeekableInput input = new FsInput(path, config);
            GenericDatumReader<GenericRecord> fileReader = new GenericDatumReader<GenericRecord>();
            FileReader<GenericRecord> reader = DataFileReader.openReader(input, fileReader);

            Schema schema = null;
            for (GenericRecord datum : reader) {
                schema = datum.getSchema();
                break;
            }

            AvroJob.setInputKeySchema(job, schema);
            AvroJob.setMapOutputValueSchema(job, schema);
            AvroJob.setOutputKeySchema(job, schema);
            AvroKeyOutputFormat.setCompressOutput(job,
                    Boolean.valueOf(properties.getProperty(EventDataSamplingProperty.COMPRESS_SAMPLE.name(), "true")));

            SamplingConfiguration samplingConfig = JsonHelper.deserialize(samplingConfigStr, SamplingConfiguration.class);
            
            for (SamplingElement samplingElement : samplingConfig.getSamplingElements()) {
                AvroMultipleOutputs.addNamedOutput(job, samplingElement.getName() + "Training", AvroKeyOutputFormat.class, schema);
                AvroMultipleOutputs.addNamedOutput(job, samplingElement.getName() + "Test", AvroKeyOutputFormat.class, schema);
            }

            job.setInputFormatClass(AvroKeyInputFormat.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(AvroValue.class);
            job.setMapperClass(EventDataSamplingMapper.class);
            job.setReducerClass(EventDataSamplingReducer.class);
            job.setOutputKeyClass(AvroKey.class);
            job.setOutputValueClass(NullWritable.class);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

}
