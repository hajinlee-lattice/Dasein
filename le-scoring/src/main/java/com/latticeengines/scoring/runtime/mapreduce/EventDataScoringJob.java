package com.latticeengines.scoring.runtime.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.JSONObject;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MRJobCustomization;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class EventDataScoringJob extends Configured implements Tool, MRJobCustomization {

    private static final String SCORING_JOB_TYPE = "scoringJob";

    private static final String dataTypeFile = "datatype.avsc";

    private static String comma = ",";

    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    private static final String scoringPythonPath = "/app/scoring/scripts/scoring.py";

    private static final Log log = LogFactory.getLog(EventDataScoringJob.class);

    public EventDataScoringJob(Configuration config) {
        setConf(config);
    }

    public EventDataScoringJob(Configuration config, MapReduceCustomizationRegistry mapReduceCustomizationRegistry) {
        setConf(config);
        this.mapReduceCustomizationRegistry = mapReduceCustomizationRegistry;
        this.mapReduceCustomizationRegistry.register(this);
    }

    @Override
    public String getJobType() {
        return SCORING_JOB_TYPE;
    }

    @Override
    public void customize(Job mrJob, Properties properties) {
        try {
            Configuration config = mrJob.getConfiguration();
            config.set(ScoringProperty.LEAD_INPUT_QUEUE_ID.name(),
                    properties.getProperty(ScoringProperty.LEAD_INPUT_QUEUE_ID.name()));
            config.set(ScoringProperty.TENANT_ID.name(), properties.getProperty(ScoringProperty.TENANT_ID.name()));
            config.set(ScoringProperty.LOG_DIR.name(), properties.getProperty(ScoringProperty.LOG_DIR.name()));

            String queueName = properties.getProperty(MapReduceProperty.QUEUE.name());
            config.set("mapreduce.job.queuename", queueName);
            String inputDir = properties.getProperty(MapReduceProperty.INPUT.name());
            AvroKeyInputFormat.setInputPathFilter(mrJob, IgnoreDirectoriesAndSupportOnlyAvroFilesFilter.class);
            AvroKeyInputFormat.addInputPath(mrJob, new Path(inputDir));
            AvroKeyInputFormat.setMaxInputSplitSize(mrJob,
                    Long.valueOf(properties.getProperty(MapReduceProperty.MAX_INPUT_SPLIT_SIZE.name())));

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

            String dataTypeFilePath = inputDir + "/" + dataTypeFile;
            generateDataTypeSchema(schema, dataTypeFilePath, config);

            String leadInputFileThreshold = properties.getProperty(ScoringProperty.LEAD_FILE_THRESHOLD.name());
            config.setLong(ScoringProperty.LEAD_FILE_THRESHOLD.name(), Long.parseLong(leadInputFileThreshold));
            String outputDir = properties.getProperty(MapReduceProperty.OUTPUT.name());
            config.set(MapReduceProperty.OUTPUT.name(), outputDir);
            mrJob.setInputFormatClass(AvroKeyInputFormat.class);
            mrJob.setOutputFormatClass(NullOutputFormat.class);
            mrJob.setMapperClass(EventDataScoringMapper.class);
            mrJob.setNumReduceTasks(0);

            String customer = properties.getProperty(MapReduceProperty.CUSTOMER.name());
            if (properties.getProperty(MapReduceProperty.CACHE_FILE_PATH.name()) != null) {
                String[] cachePaths = properties.getProperty(MapReduceProperty.CACHE_FILE_PATH.name()).split(comma);
                URI[] cacheFiles = new URI[cachePaths.length];
                for (int i = 0; i < cacheFiles.length; i++) {
                    int idx = cachePaths[i].indexOf(customer);
                    // ${customer}/models/${table_name}/model_id
                    String id = cachePaths[i].substring(idx).split("/")[3];
                    cacheFiles[i] = new URI(cachePaths[i].trim() + "#" + id);
                }
                mrJob.setCacheFiles(cacheFiles);
            }
            mrJob.addCacheFile(new URI(dataTypeFilePath));
            mrJob.addCacheFile(new URI(scoringPythonPath));

            if (properties.getProperty(MapReduceProperty.CACHE_ARCHIVE_PATH.name()) != null) {
                String[] cacheArchivePaths = properties.getProperty(MapReduceProperty.CACHE_ARCHIVE_PATH.name()).split(
                        comma);
                URI[] cacheArchives = new URI[cacheArchivePaths.length];
                for (int i = 0; i < cacheArchives.length; i++) {
                    cacheArchives[i] = new URI(cacheArchivePaths[i].trim());
                }
                mrJob.setCacheArchives(cacheArchives);
            }

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

    @SuppressWarnings("unchecked")
    @VisibleForTesting
    void generateDataTypeSchema(Schema schema, String dataTypeFilePath, Configuration config) {
        List<Field> fields = schema.getFields();
        JSONObject jsonObj = new JSONObject();
        for (Field field : fields) {
            String type = field.schema().getTypes().get(0).getName();
            if (type.equals("string") || type.equals("bytes"))
                jsonObj.put(field.name(), 1);
            else
                jsonObj.put(field.name(), 0);
        }
        try {
            HdfsUtils.writeToFile(config, dataTypeFilePath, jsonObj.toJSONString());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        JobConf jobConf = new JobConf(getConf(), getClass());
        @SuppressWarnings("deprecation")
        Job job = new Job(jobConf, "scoringJob");

        Properties properties = new Properties();
        properties.setProperty(MapReduceProperty.CUSTOMER.name(), args[0]);
        properties.setProperty(MapReduceProperty.QUEUE.name(), "Scoring");
        properties.setProperty(MapReduceProperty.INPUT.name(), args[1]);
        properties.setProperty(MapReduceProperty.OUTPUT.name(), args[2]);
        properties.setProperty(MapReduceProperty.CACHE_FILE_PATH.name(), args[4]);
        properties.setProperty(MapReduceProperty.MAX_INPUT_SPLIT_SIZE.name(), args[5]);
        properties.setProperty(ScoringProperty.LEAD_FILE_THRESHOLD.name(), args[6]);
        properties.setProperty(ScoringProperty.LEAD_INPUT_QUEUE_ID.name(), args[7]);
        properties.setProperty(ScoringProperty.TENANT_ID.name(), args[8]);
        properties.setProperty(ScoringProperty.LOG_DIR.name(), args[9]);

        customize(job, properties);
        if (job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new EventDataScoringJob(new Configuration()), args);
        System.exit(res);
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
