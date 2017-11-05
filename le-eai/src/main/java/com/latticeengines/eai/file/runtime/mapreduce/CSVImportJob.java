package com.latticeengines.eai.file.runtime.mapreduce;

import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.yarn.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.yarn.exposed.mapreduce.MRJobUtil;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.yarn.exposed.runtime.mapreduce.MRJobCustomizationBase;

public class CSVImportJob extends MRJobCustomizationBase {

    public static final String CSV_IMPORT_JOB_TYPE = "eaiCSVImportJob";

    public static final String MAPRED_MAP_TASKS_PROPERTY = "mapreduce.job.maps";

    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    public CSVImportJob(Configuration config) {
        super(config);
    }

    public CSVImportJob(Configuration config, //
            MapReduceCustomizationRegistry mapReduceCustomizationRegistry) {
        this(config);
        this.mapReduceCustomizationRegistry = mapReduceCustomizationRegistry;
        this.mapReduceCustomizationRegistry.register(this);
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

            config.set("eai.dedup.enable", properties.getProperty("eai.dedup.enable"));

            config.set("eai.id.column.name", properties.getProperty("eai.id.column.name"));

            // get schema
            Table table = JsonUtils.deserialize(tableSchema, Table.class);
            Schema schema = TableUtils.createSchema(table.getName(), table);
            AvroJob.setOutputKeySchema(mrJob, schema);

            String outputDir = properties.getProperty(MapReduceProperty.OUTPUT.name());
            config.set(MapReduceProperty.OUTPUT.name(), outputDir);
            MapFileOutputFormat.setOutputPath(mrJob, new Path(outputDir));

            mrJob.setInputFormatClass(TextInputFormat.class);
            mrJob.setOutputFormatClass(NullOutputFormat.class);
            mrJob.setMapperClass(CSVImportMapper.class);
            mrJob.setNumReduceTasks(0);

            TextInputFormat.setMinInputSplitSize(mrJob, 100000000000L);

            MRJobUtil.setLocalizedResources(mrJob, properties);
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
