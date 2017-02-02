package com.latticeengines.eai.service.impl.dynamodb;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
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
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("dynamoExportService")
public class DynamoExportServiceImpl extends ExportService {

    private static final Log log = LogFactory.getLog(DynamoExportServiceImpl.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private EaiYarnService eaiYarnService;

    protected DynamoExportServiceImpl() {
        super(ExportDestination.DYNAMO);
    }

    @Override
    public void exportDataFromHdfs(ExportConfiguration exportConfig, ExportContext context) {
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
            context.setProperty(ExportProperty.INPUT_FILE_PATH,
                    ExtractUtils.getSingleExtractPath(yarnConfiguration, table));
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
        context.setProperty(DynamoExportJob.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED,
                exportConfig.getProperties().get(DynamoExportJob.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED));
        context.setProperty(DynamoExportJob.CONFIG_AWS_SECRET_KEY_ENCRYPTED,
                exportConfig.getProperties().get(DynamoExportJob.CONFIG_AWS_SECRET_KEY_ENCRYPTED));

        log.info(String.format("Exporting data for table %s at input path %s", table,
                context.getProperty(ExportProperty.INPUT_FILE_PATH, String.class)));
        Properties props = getProperties(context, table);

        ApplicationId appId = eaiYarnService.submitMRJob(DynamoExportJob.DYNAMO_EXPORT_JOB_TYPE, props);
        context.setProperty(ImportProperty.APPID, appId);
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

        props.setProperty("eai.table.schema", JsonUtils.serialize(table));

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

        return props;
    }

}
