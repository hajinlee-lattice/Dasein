package com.latticeengines.eai.runtime.mapreduce;

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
import org.apache.hadoop.mapreduce.Mapper;
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

public abstract class AvroExportJob extends Configured implements Tool, MRJobCustomization {

    public static final String CSV_EXPORT_JOB_TYPE = "eaiCSVExportJob";

    public static final String MAPRED_MAP_TASKS_PROPERTY = "mapreduce.job.maps";

    private static final String dependencyPath = "/app/";

    private static final String jarDependencyPath = "/eai/lib";

    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    private VersionManager versionManager;

    private String stackName;

    public AvroExportJob(Configuration config) {
        setConf(config);
    }

    public AvroExportJob(Configuration config, //
            MapReduceCustomizationRegistry mapReduceCustomizationRegistry, //
            VersionManager versionManager, String stackName) {
        this(config);
        this.mapReduceCustomizationRegistry = mapReduceCustomizationRegistry;
        this.mapReduceCustomizationRegistry.register(this);
        this.versionManager = versionManager;
        this.stackName = stackName;
    }

    @SuppressWarnings("rawtypes")
    protected abstract Class<? extends Mapper> getMapperClass();

    protected abstract int getNumMappers();

    public abstract String getJobType();

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

            String tableSchema = properties.getProperty("eai.table.schema");
            if (tableSchema != null) {
                config.set("eai.table.schema", tableSchema);
            }

            mrJob.setInputFormatClass(AvroKeyInputFormat.class);
            mrJob.setOutputFormatClass(NullOutputFormat.class);
            mrJob.setMapperClass(getMapperClass());
            mrJob.setNumReduceTasks(0);
            if (getNumMappers() == 1) {
                AvroKeyInputFormat.setMinInputSplitSize(mrJob, 100000000000L);
            }
            config.setInt(MAPRED_MAP_TASKS_PROPERTY, getNumMappers());

            MRJobUtil.setLocalizedResources(mrJob, properties);
            List<String> jarFilePaths = HdfsUtils.getFilesForDir(mrJob.getConfiguration(), dependencyPath
                    + versionManager.getCurrentVersionInStack(stackName) + jarDependencyPath, ".*.jar$");
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
