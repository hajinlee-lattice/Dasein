package com.latticeengines.eai.file.runtime.mapreduce;

import java.io.IOException;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MRJobCustomization;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.dataplatform.exposed.mapreduce.MRJobUtil;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class CSVExportJob extends Configured implements Tool, MRJobCustomization {

    public static final String CSV_EXPORT_JOB_TYPE = "eaiCSVExportJob";

    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    public static final String PROP_MAPRED_MAP_TASKS = "mapred.map.tasks";

    // pUBLIC static final String MAP_INPUT_SCHEMA = "avro.map.input.schema";

    private VersionManager versionManager;

    private static final String dependencyPath = "/app/";

    private static final String jarDependencyPath = "/eai/lib";

    public CSVExportJob(Configuration config) {
        setConf(config);
    }

    public CSVExportJob(Configuration config, MapReduceCustomizationRegistry mapReduceCustomizationRegistry,
            VersionManager versionManager) {
        this(config);
        this.mapReduceCustomizationRegistry = mapReduceCustomizationRegistry;
        this.mapReduceCustomizationRegistry.register(this);
        this.versionManager = versionManager;
    }

    @Override
    public String getJobType() {
        return CSV_EXPORT_JOB_TYPE;
    }

    @Override
    public void customize(Job mrJob, Properties properties) {
        try {
            Configuration config = mrJob.getConfiguration();

            String queueName = properties.getProperty(MapReduceProperty.QUEUE.name());
            config.set("mapreduce.job.queuename", queueName);

            String inputDir = properties.getProperty(MapReduceProperty.INPUT.name());
            AvroKeyInputFormat.setInputPathFilter(mrJob, IgnoreDirectoriesAndSupportOnlyAvroFilesFilter.class);
            AvroKeyInputFormat.addInputPath(mrJob, new Path(inputDir));
            List<String> files = HdfsUtils.getFilesForDir(mrJob.getConfiguration(), inputDir, ".*.avro$");
            String filename = files.size() > 0 ? files.get(0) : null;
            if (filename == null) {
                throw new LedpException(LedpCode.LEDP_12003, new String[] { inputDir });
            }
            Path path = new Path(filename);
            Schema schema = AvroUtils.getSchema(config, path);
            AvroJob.setInputKeySchema(mrJob, schema);

            String outputDir = properties.getProperty(MapReduceProperty.OUTPUT.name());
            config.set(MapReduceProperty.OUTPUT.name(), outputDir);

            mrJob.setInputFormatClass(AvroKeyInputFormat.class);
            mrJob.setOutputFormatClass(NullOutputFormat.class);
            mrJob.setMapperClass(CSVExportMapper.class);
            mrJob.setNumReduceTasks(0);
            mrJob.getConfiguration().setInt(PROP_MAPRED_MAP_TASKS, 1);

            MRJobUtil.setLocalizedResources(mrJob, properties);
            List<String> jarFilePaths = HdfsUtils.getFilesForDir(mrJob.getConfiguration(), dependencyPath
                    + versionManager.getCurrentVersion() + jarDependencyPath, ".*.jar$");
            for (String jarFilePath : jarFilePaths) {
                mrJob.addFileToClassPath(new Path(jarFilePath));
            }

            // config.set(MRJobConfig.MAP_JAVA_OPTS,
            // "-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=4001,server=y,suspend=y");

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        return 0;
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
