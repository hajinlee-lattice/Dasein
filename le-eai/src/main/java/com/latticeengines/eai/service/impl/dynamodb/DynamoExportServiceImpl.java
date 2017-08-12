package com.latticeengines.eai.service.impl.dynamodb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.eai.dynamodb.runtime.DynamoExportJob;
import com.latticeengines.eai.service.EaiYarnService;
import com.latticeengines.eai.service.ExportService;
import com.latticeengines.eai.util.EaiJobUtil;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;

@Component("dynamoExportService")
public class DynamoExportServiceImpl extends ExportService {

    private static final Logger log = LoggerFactory.getLogger(DynamoExportServiceImpl.class);

    @Autowired
    private EaiYarnService eaiYarnService;

    @Autowired
    private VersionManager versionManager;

    @Value("${dataplatform.hdfs.stack:}")
    protected String stackName;

    protected DynamoExportServiceImpl() {
        super(ExportDestination.DYNAMO);
    }

    @Override
    public void exportDataFromHdfs(ExportConfiguration exportConfig, ExportContext context) {
        Properties props = constructProperties(exportConfig, context);
        ApplicationId appId = eaiYarnService.submitMRJob(DynamoExportJob.DYNAMO_EXPORT_JOB_TYPE, props);
        context.setProperty(ImportProperty.APPID, appId);
    }

    public Properties constructProperties(ExportConfiguration exportConfig, ExportContext context) {
        if (StringUtils.isNotEmpty(exportConfig.getExportTargetPath())) {
            context.setProperty(ExportProperty.TARGETPATH, exportConfig.getExportTargetPath());
        } else {
            String targetPath = exportConfig.getProperties().get(ExportProperty.TARGET_DIRECTORY);
            context.setProperty(ExportProperty.TARGETPATH, targetPath);
        }

        context.setProperty(ExportProperty.CUSTOMER, exportConfig.getCustomerSpace().toString());
        Table table = exportConfig.getTable();

        String inputPath = exportConfig.getExportInputPath();
        if (StringUtils.isNotEmpty(inputPath)) {
            context.setProperty(ExportProperty.INPUT_FILE_PATH, inputPath);
        } else {
            context.setProperty(ExportProperty.INPUT_FILE_PATH, ExtractUtils.getSingleExtractPath(
                    context.getProperty(ExportProperty.HADOOPCONFIG, Configuration.class), table));
        }
        boolean exportUsingDisplayName = exportConfig.getUsingDisplayName();
        context.setProperty(ExportProperty.EXPORT_USING_DISPLAYNAME, String.valueOf(exportUsingDisplayName));

        context.setProperty(ExportProperty.NUM_MAPPERS, exportConfig.getProperties().get(ExportProperty.NUM_MAPPERS));

        context.setProperty(DynamoExportJob.CONFIG_REPOSITORY,
                exportConfig.getProperties().get(DynamoExportJob.CONFIG_REPOSITORY));
        context.setProperty(DynamoExportJob.CONFIG_RECORD_TYPE,
                exportConfig.getProperties().get(DynamoExportJob.CONFIG_RECORD_TYPE));
        context.setProperty(DynamoExportJob.CONFIG_ENTITY_CLASS_NAME,
                exportConfig.getProperties().get(DynamoExportJob.CONFIG_ENTITY_CLASS_NAME));

        context.setProperty(DynamoExportJob.CONFIG_ENDPOINT,
                exportConfig.getProperties().get(DynamoExportJob.CONFIG_ENDPOINT));
        context.setProperty(DynamoExportJob.CONFIG_AWS_REGION,
                exportConfig.getProperties().get(DynamoExportJob.CONFIG_AWS_REGION));
        context.setProperty(DynamoExportJob.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED,
                exportConfig.getProperties().get(DynamoExportJob.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED));
        context.setProperty(DynamoExportJob.CONFIG_AWS_SECRET_KEY_ENCRYPTED,
                exportConfig.getProperties().get(DynamoExportJob.CONFIG_AWS_SECRET_KEY_ENCRYPTED));

        log.info(String.format("Exporting data for table %s at input path %s", table,
                context.getProperty(ExportProperty.INPUT_FILE_PATH, String.class)));
        return getProperties(context, table);
    }

    private Properties getProperties(ExportContext ctx, Table table) {
        Properties props = new Properties();
        props.setProperty(MapReduceProperty.QUEUE.name(), LedpQueueAssigner.getPropDataQueueNameForSubmission());

        String customer = ctx.getProperty(ExportProperty.CUSTOMER, String.class);
        props.setProperty(MapReduceProperty.CUSTOMER.name(), customer);

        String inputPath = ctx.getProperty(ExportProperty.INPUT_FILE_PATH, String.class);
        props.setProperty(MapReduceProperty.INPUT.name(), inputPath);

        String targetHdfsPath = ctx.getProperty(ExportProperty.TARGETPATH, String.class);
        props.setProperty(MapReduceProperty.OUTPUT.name(), targetHdfsPath);

        props.setProperty(ExportProperty.NUM_MAPPERS, ctx.getProperty(ExportProperty.NUM_MAPPERS, String.class));

        if (table != null) {
            props.setProperty("eai.table.schema", JsonUtils.serialize(table));
        }

        props.setProperty(DynamoExportJob.CONFIG_RECORD_TYPE,
                ctx.getProperty(DynamoExportJob.CONFIG_RECORD_TYPE, String.class));
        props.setProperty(DynamoExportJob.CONFIG_REPOSITORY,
                ctx.getProperty(DynamoExportJob.CONFIG_REPOSITORY, String.class));
        props.setProperty(DynamoExportJob.CONFIG_ENTITY_CLASS_NAME,
                ctx.getProperty(DynamoExportJob.CONFIG_ENTITY_CLASS_NAME, String.class));

        if (ctx.getProperty(DynamoExportJob.CONFIG_ENDPOINT, String.class) != null) {
            props.setProperty(DynamoExportJob.CONFIG_ENDPOINT,
                    ctx.getProperty(DynamoExportJob.CONFIG_ENDPOINT, String.class));
        } else {
            props.setProperty(DynamoExportJob.CONFIG_ENDPOINT, "");
        }
        if (ctx.getProperty(DynamoExportJob.CONFIG_AWS_REGION, String.class) != null) {
            props.setProperty(DynamoExportJob.CONFIG_AWS_REGION,
                    ctx.getProperty(DynamoExportJob.CONFIG_AWS_REGION, String.class));
        } else {
            props.setProperty(DynamoExportJob.CONFIG_AWS_REGION, "us-east-1");
        }
        if (ctx.getProperty(DynamoExportJob.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED, String.class) != null) {
            props.setProperty(DynamoExportJob.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED,
                    ctx.getProperty(DynamoExportJob.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED, String.class));
        } else {
            props.setProperty(DynamoExportJob.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED, "");
        }
        if (ctx.getProperty(DynamoExportJob.CONFIG_AWS_SECRET_KEY_ENCRYPTED, String.class) != null) {
            props.setProperty(DynamoExportJob.CONFIG_AWS_SECRET_KEY_ENCRYPTED,
                    ctx.getProperty(DynamoExportJob.CONFIG_AWS_SECRET_KEY_ENCRYPTED, String.class));
        } else {
            props.setProperty(DynamoExportJob.CONFIG_AWS_SECRET_KEY_ENCRYPTED, "");
        }
        List<String> cacheFiles = new ArrayList<>();
        try {
            cacheFiles = EaiJobUtil.getCacheFiles(ctx.getProperty(ExportProperty.HADOOPCONFIG, Configuration.class),
                    versionManager.getCurrentVersionInStack(stackName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        props.setProperty(MapReduceProperty.CACHE_FILE_PATH.name(), String.join(",", cacheFiles));

        return props;
    }

}
