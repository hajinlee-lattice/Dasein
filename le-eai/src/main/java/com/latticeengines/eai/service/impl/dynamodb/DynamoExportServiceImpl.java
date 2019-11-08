package com.latticeengines.eai.service.impl.dynamodb;

import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ATLAS_LOOKUP_IDS;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ATLAS_TENANT;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_AWS_REGION;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_AWS_SECRET_KEY_ENCRYPTED;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ENDPOINT;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_ENTITY_CLASS_NAME;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_KEY_PREFIX;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_PARTITION_KEY;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_RECORD_TYPE;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_REPOSITORY;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_SORT_KEY;
import static com.latticeengines.domain.exposed.eai.HdfsToDynamoConfiguration.CONFIG_TABLE_NAME;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.ExtractUtils;
import com.latticeengines.eai.dynamodb.runtime.AtlasLookupCacheExportJob;
import com.latticeengines.eai.dynamodb.runtime.DynamoExportJob;
import com.latticeengines.eai.service.EaiYarnService;
import com.latticeengines.eai.service.ExportService;
import com.latticeengines.eai.util.EaiJobUtil;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;

@Component("dynamoExportService")
public class DynamoExportServiceImpl extends ExportService {

    private static final Logger log = LoggerFactory.getLogger(DynamoExportServiceImpl.class);

    @Inject
    private EaiYarnService eaiYarnService;

    @Inject
    private VersionManager versionManager;

    @Value("${dataplatform.hdfs.stack:}")
    protected String stackName;

    @Value("${eai.export.dynamo.atlas.lookup.table}")
    private String atlasLookupCacheTableName;

    protected DynamoExportServiceImpl() {
        super(ExportDestination.DYNAMO);
    }

    @Override
    public void exportDataFromHdfs(ExportConfiguration exportConfig, ExportContext context) {
        boolean toAtlasLookupCache = exportConfig.getProperties().containsKey(CONFIG_ATLAS_LOOKUP_IDS);
        Properties props = constructProperties(exportConfig, context);
        ApplicationId appId;
        if (toAtlasLookupCache) {
            log.info("Submitting AtlasLookupCacheExportJob");
            appId = eaiYarnService.submitMRJob(AtlasLookupCacheExportJob.DYNAMO_EXPORT_JOB_TYPE, props);
        } else {
            log.info("Submitting DynamoExportJob");
            appId = eaiYarnService.submitMRJob(DynamoExportJob.DYNAMO_EXPORT_JOB_TYPE, props);
        }
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

        boolean toAtlasLookupCache = exportConfig.getProperties().containsKey(CONFIG_ATLAS_LOOKUP_IDS);
        if (toAtlasLookupCache) {
            String dynamoTableName = exportConfig.getProperties().get(CONFIG_TABLE_NAME);
            if (StringUtils.isBlank(dynamoTableName)) {
                dynamoTableName = atlasLookupCacheTableName;
            }
            context.setProperty(CONFIG_TABLE_NAME, dynamoTableName);
            String tenant = CustomerSpace.shortenCustomerSpace(exportConfig.getCustomerSpace().toString());
            context.setProperty(CONFIG_ATLAS_TENANT, tenant);
            context.setProperty(CONFIG_ATLAS_LOOKUP_IDS, exportConfig.getProperties().get(CONFIG_ATLAS_LOOKUP_IDS));
        } else {
            context.setProperty(CONFIG_REPOSITORY, exportConfig.getProperties().get(CONFIG_REPOSITORY));
            context.setProperty(CONFIG_RECORD_TYPE, exportConfig.getProperties().get(CONFIG_RECORD_TYPE));
            context.setProperty(CONFIG_ENTITY_CLASS_NAME, exportConfig.getProperties().get(CONFIG_ENTITY_CLASS_NAME));
            context.setProperty(CONFIG_KEY_PREFIX, exportConfig.getProperties().get(CONFIG_KEY_PREFIX));
            context.setProperty(CONFIG_PARTITION_KEY, exportConfig.getProperties().get(CONFIG_PARTITION_KEY));
            context.setProperty(CONFIG_SORT_KEY, exportConfig.getProperties().get(CONFIG_SORT_KEY));
        }

        context.setProperty(CONFIG_ENDPOINT, exportConfig.getProperties().get(CONFIG_ENDPOINT));
        context.setProperty(CONFIG_AWS_REGION, exportConfig.getProperties().get(CONFIG_AWS_REGION));
        context.setProperty(CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED,
                exportConfig.getProperties().get(CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED));
        context.setProperty(CONFIG_AWS_SECRET_KEY_ENCRYPTED,
                exportConfig.getProperties().get(CONFIG_AWS_SECRET_KEY_ENCRYPTED));

        log.info(String.format("Exporting data for table %s at input path %s", table,
                context.getProperty(ExportProperty.INPUT_FILE_PATH, String.class)));
        return getProperties(context, table);
    }

    private Properties getProperties(ExportContext ctx, Table table) {
        boolean toAtlasLookupCache = ctx.containsProperty(CONFIG_ATLAS_LOOKUP_IDS);

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

        if (toAtlasLookupCache) {
            props.setProperty(CONFIG_TABLE_NAME, ctx.getProperty(CONFIG_TABLE_NAME, String.class));
            props.setProperty(CONFIG_ATLAS_TENANT, ctx.getProperty(CONFIG_ATLAS_TENANT, String.class));
            props.setProperty(CONFIG_ATLAS_LOOKUP_IDS, ctx.getProperty(CONFIG_ATLAS_LOOKUP_IDS, String.class));
        } else {
            props.setProperty(CONFIG_RECORD_TYPE, ctx.getProperty(CONFIG_RECORD_TYPE, String.class));
            props.setProperty(CONFIG_REPOSITORY, ctx.getProperty(CONFIG_REPOSITORY, String.class));
            props.setProperty(CONFIG_ENTITY_CLASS_NAME, ctx.getProperty(CONFIG_ENTITY_CLASS_NAME, String.class));
            props.setProperty(CONFIG_KEY_PREFIX, ctx.getProperty(CONFIG_KEY_PREFIX, String.class, ""));
            props.setProperty(CONFIG_PARTITION_KEY, ctx.getProperty(CONFIG_PARTITION_KEY, String.class, ""));
            props.setProperty(CONFIG_SORT_KEY, ctx.getProperty(CONFIG_SORT_KEY, String.class, ""));
        }

        props.setProperty(CONFIG_ENDPOINT, ctx.getProperty(CONFIG_ENDPOINT, String.class, ""));
        props.setProperty(CONFIG_AWS_REGION,
                ctx.getProperty(CONFIG_AWS_REGION, String.class, "us-east-1"));
        props.setProperty(CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED,
                ctx.getProperty(CONFIG_AWS_ACCESS_KEY_ID_ENCRYPTED, String.class, ""));
        props.setProperty(CONFIG_AWS_SECRET_KEY_ENCRYPTED,
                ctx.getProperty(CONFIG_AWS_SECRET_KEY_ENCRYPTED, String.class, ""));

        List<String> cacheFiles;
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
