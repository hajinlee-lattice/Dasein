package com.latticeengines.eai.runtime.mapreduce;

import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.yarn.exposed.client.mapreduce.MapReduceCustomizationRegistry;
import com.latticeengines.yarn.exposed.mapreduce.MRJobUtil;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.yarn.exposed.runtime.mapreduce.MRJobCustomizationBase;

public abstract class AvroExportJob extends MRJobCustomizationBase {

    private static final Logger log = LoggerFactory.getLogger(AvroExportJob.class);

    public static final String CSV_EXPORT_JOB_TYPE = "eaiCSVExportJob";

    public static final String MAPRED_MAP_TASKS_PROPERTY = "mapreduce.job.maps";

    private MapReduceCustomizationRegistry mapReduceCustomizationRegistry;

    public AvroExportJob(Configuration config) {
        super(config);
    }

    public AvroExportJob(Configuration config, //
            MapReduceCustomizationRegistry mapReduceCustomizationRegistry) {
        this(config);
        this.mapReduceCustomizationRegistry = mapReduceCustomizationRegistry;
        this.mapReduceCustomizationRegistry.register(this);
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
            inputDir = PathUtils.toDirWithoutTrailingSlash(inputDir);
            log.info("Reading from inputDir=" + inputDir);
            AvroKeyInputFormat.addInputPath(mrJob, new Path(inputDir));
            AvroKeyInputFormat.setInputPathFilter(mrJob, IgnoreDirectoriesAndSupportOnlyAvroFilesFilter.class);

            String fileGlob = PathUtils.toAvroGlob(inputDir);
            log.info("Extracting schema from glob: " + fileGlob);
            Schema schema = AvroUtils.getSchemaFromGlob(config, fileGlob);
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
                AvroKeyInputFormat.setMinInputSplitSize(mrJob, Long.MAX_VALUE);
            } else {
                AvroKeyInputFormat.setMinInputSplitSize(mrJob, 104857600L);
                AvroKeyInputFormat.setMaxInputSplitSize(mrJob, 10737418240L);
                config.set("mapreduce.job.running.map.limit", String.valueOf(getNumMappers()));
                config.set("mapreduce.tasktracker.map.tasks.maximum", String.valueOf(getNumMappers()));
            }
            config.setInt(MAPRED_MAP_TASKS_PROPERTY, getNumMappers());
            log.info("Set num mappers to " + getNumMappers());

            MRJobUtil.setLocalizedResources(mrJob, properties);

            String opts = config.get(MRJobConfig.MAP_JAVA_OPTS, "");
            config.set(MRJobConfig.MAP_JAVA_OPTS,
                    opts + " -Dlog4j.configurationFile=log4j2-yarn.xml" //
                            + " -DLOG4J_LE_LEVEL=INFO " + CipherUtils.getSecretPropertyStr());

        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        return 0;
    }

}
