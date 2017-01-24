package com.latticeengines.eai.file.runtime.mapreduce;

import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MRJobCustomization;
import com.latticeengines.dataplatform.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.dataplatform.exposed.mapreduce.MRJobUtil;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.TableUtils;

public class CSVImportJob extends Configured implements Tool, MRJobCustomization {

    public static final String CSV_IMPORT_JOB_TYPE = "eaiCSVImportJob";

    public static final String MAPRED_MAP_TASKS_PROPERTY = "mapreduce.job.maps";

    private static final String dependencyPath = "/app/";

    private static final String jarDependencyPath = "/eai/lib";

    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    private VersionManager versionManager;

    private String stackName;

    public CSVImportJob(Configuration config) {
        setConf(config);
    }

    public CSVImportJob(Configuration config, //
            MapReduceCustomizationRegistry mapReduceCustomizationRegistry, //
            VersionManager versionManager, String stackName) {
        this(config);
        this.mapReduceCustomizationRegistry = mapReduceCustomizationRegistry;
        this.mapReduceCustomizationRegistry.register(this);
        this.versionManager = versionManager;
        this.stackName = stackName;
    }

    @Override
    public String getJobType() {
        return CSV_IMPORT_JOB_TYPE;
    }

    @Override
    public void customize(Job mrJob, Properties properties) {
        try {
            Configuration config = mrJob.getConfiguration();

            String queueName = properties.getProperty(MapReduceProperty.QUEUE.name());
            config.set("mapreduce.job.queuename", queueName);

            String inputDir = properties.getProperty(MapReduceProperty.INPUT.name());
            TextInputFormat.addInputPath(mrJob, new Path(inputDir));

            String tableSchema = properties.getProperty("eai.table.schema");
            config.set("eai.table.schema", tableSchema);

            // get schema
            Table table = JsonUtils.deserialize(tableSchema, Table.class);
            Schema schema = TableUtils.createSchema(table.getName(), table);
            System.out.println(schema.toString());
            AvroJob.setOutputKeySchema(mrJob, schema);

            String outputDir = properties.getProperty(MapReduceProperty.OUTPUT.name());
            config.set(MapReduceProperty.OUTPUT.name(), outputDir);
            MapFileOutputFormat.setOutputPath(mrJob, new Path(outputDir));

            mrJob.setInputFormatClass(TextInputFormat.class);
            mrJob.setOutputFormatClass(NullOutputFormat.class);
            mrJob.setMapperClass(CSVImportMapper.class);
            mrJob.setNumReduceTasks(0);

            TextInputFormat.setMinInputSplitSize(mrJob, 100000000000L);

            mrJob.addFileToClassPath(new Path(inputDir));
            MRJobUtil.setLocalizedResources(mrJob, properties);
            List<String> jarFilePaths = HdfsUtils.getFilesForDir(mrJob.getConfiguration(),
                    dependencyPath + versionManager.getCurrentVersionInStack(stackName) + jarDependencyPath, ".*.jar$");
            for (String jarFilePath : jarFilePaths) {
                mrJob.addFileToClassPath(new Path(jarFilePath));
            }

            // config.set(MRJobConfig.MAP_JAVA_OPTS,
            // "-Xdebug -Xnoagent -Djava.compiler=NONE
            // -Xrunjdwp:transport=dt_socket,address=4001,server=y,suspend=y");

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

}
