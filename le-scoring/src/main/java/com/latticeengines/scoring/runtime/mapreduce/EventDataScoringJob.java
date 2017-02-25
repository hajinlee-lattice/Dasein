package com.latticeengines.scoring.runtime.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
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

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MRJobCustomization;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.dataplatform.exposed.mapreduce.MRJobUtil;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;

public class EventDataScoringJob extends Configured implements Tool, MRJobCustomization {

    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    private VersionManager versionManager;

    private String stackName;

    private static final String dependencyPath = "/app/";

    private static final String jarDependencyPath = "/scoring/lib";

    private static final String scoringPythonPath = "/scoring/scripts/scoring.py";
    
    private static final String pythonLauncherPath = "/dataplatform/scripts/pythonlauncher.sh";

    public EventDataScoringJob(Configuration config) {
        setConf(config);
    }

    public EventDataScoringJob(Configuration config, MapReduceCustomizationRegistry mapReduceCustomizationRegistry,
            VersionManager versionManager, String stackName) {
        setConf(config);
        this.mapReduceCustomizationRegistry = mapReduceCustomizationRegistry;
        this.mapReduceCustomizationRegistry.register(this);
        this.versionManager = versionManager;
        this.stackName = stackName;
    }

    @Override
    public String getJobType() {
        return ScoringDaemonService.SCORING_JOB_TYPE;
    }

    @Override
    public void customize(Job mrJob, Properties properties) {
        try {
            Configuration config = mrJob.getConfiguration();
            config.set(ScoringProperty.UNIQUE_KEY_COLUMN.name(),
                    properties.getProperty(ScoringProperty.UNIQUE_KEY_COLUMN.name()));
            config.set(ScoringProperty.USE_SCOREDERIVATION.name(),
                    properties.getProperty(ScoringProperty.USE_SCOREDERIVATION.name()));
            if (properties.containsKey(ScoringProperty.MODEL_GUID.name())) {
                config.set(ScoringProperty.MODEL_GUID.name(), properties.getProperty(ScoringProperty.MODEL_GUID.name()));
            }
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
            AvroKeyInputFormat.setMinInputSplitSize(mrJob,
                    Long.valueOf(properties.getProperty(MapReduceProperty.MIN_INPUT_SPLIT_SIZE.name())));

            List<String> files = HdfsUtils.getFilesForDir(mrJob.getConfiguration(), inputDir, ".*.avro$");
            String filename = files.size() > 0 ? files.get(0) : null;
            if (filename == null) {
                throw new LedpException(LedpCode.LEDP_12003, new String[] { inputDir });
            }
            Path path = new Path(filename);
            Schema schema = AvroUtils.getSchema(config, path);
            AvroJob.setInputKeySchema(mrJob, schema);

            String leadInputFileThreshold = properties.getProperty(ScoringProperty.RECORD_FILE_THRESHOLD.name());
            config.setLong(ScoringProperty.RECORD_FILE_THRESHOLD.name(), Long.parseLong(leadInputFileThreshold));
            config.set(MapReduceProperty.OUTPUT.name(), properties.getProperty(MapReduceProperty.OUTPUT.name()));
            mrJob.setInputFormatClass(AvroKeyInputFormat.class);
            mrJob.setOutputFormatClass(NullOutputFormat.class);
            mrJob.setMapperClass(EventDataScoringMapper.class);
            mrJob.setNumReduceTasks(0);

            MRJobUtil.setLocalizedResources(mrJob, properties);
            mrJob.addCacheFile(new URI(dependencyPath + versionManager.getCurrentVersionInStack(stackName)
                    + scoringPythonPath));
            mrJob.addCacheFile(new URI(dependencyPath + versionManager.getCurrentVersionInStack(stackName)
                    + pythonLauncherPath));
            List<String> jarFilePaths = HdfsUtils.getFilesForDir(mrJob.getConfiguration(), dependencyPath
                    + versionManager.getCurrentVersionInStack(stackName) + jarDependencyPath, ".*.jar$");
            for (String jarFilePath : jarFilePaths) {
                mrJob.addFileToClassPath(new Path(jarFilePath));
            }

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
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
        properties.setProperty(MapReduceProperty.MIN_INPUT_SPLIT_SIZE.name(), args[6]);
        properties.setProperty(ScoringProperty.RECORD_FILE_THRESHOLD.name(), args[7]);
        properties.setProperty(ScoringProperty.LEAD_INPUT_QUEUE_ID.name(), args[8]);
        properties.setProperty(ScoringProperty.TENANT_ID.name(), args[9]);
        properties.setProperty(ScoringProperty.LOG_DIR.name(), args[10]);
        properties.setProperty(ScoringProperty.UNIQUE_KEY_COLUMN.name(), args[11]);
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
