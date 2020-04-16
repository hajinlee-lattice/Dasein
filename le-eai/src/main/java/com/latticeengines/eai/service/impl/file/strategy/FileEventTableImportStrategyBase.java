package com.latticeengines.eai.service.impl.file.strategy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.avro.Schema.Type;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.elasticache.ElasticCacheService;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.eai.file.runtime.mapreduce.CSVFileImportProperty;
import com.latticeengines.eai.file.runtime.mapreduce.CSVImportJob;
import com.latticeengines.eai.service.EaiYarnService;
import com.latticeengines.eai.service.impl.AvroTypeConverter;
import com.latticeengines.eai.service.impl.ImportStrategy;
import com.latticeengines.eai.util.EaiJobUtil;
import com.latticeengines.eai.util.HdfsUriGenerator;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.mapreduce.MapReduceProperty;

@Component("fileEventTableImportStrategyBase")
public class FileEventTableImportStrategyBase extends ImportStrategy {

    private static final Logger log = LoggerFactory.getLogger(FileEventTableImportStrategyBase.class);

    @Inject
    private EaiYarnService eaiYarnService;

    @Inject
    private VersionManager versionManager;

    @Value("${eai.file.csv.error.lines:1000}")
    private int errorLineNumber;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    @Value("${cache.redis.command.timeout.min}")
    private int redisTimeout;

    @Value("${cache.local.redis}")
    private boolean localRedis;

    @Value("${eai.import.csv.mappers.cores}")
    private int csvImportMapperCores;

    @Value("${aws.region}")
    private String awsRegion;

    @Value("${aws.default.access.key}")
    private String awsAccessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecretKey;

    @Inject
    private ElasticCacheService elastiCacheService;

    public FileEventTableImportStrategyBase() {
        this("File.EventTable");
    }

    public FileEventTableImportStrategyBase(String key) {
        super(key);
    }

    @Override
    public void importData(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        log.info(String.format("Importing data for table %s with filter %s", table, filter));
        Properties props = getProperties(ctx, table);
        ApplicationId appId = eaiYarnService.submitMRJob(CSVImportJob.CSV_IMPORT_JOB_TYPE, props);
        // ApplicationId appId = sparkImport.launch(props);
        ctx.setProperty(ImportProperty.APPID, appId);
        updateContextProperties(ctx, table);
    }

    public void updateContextProperties(ImportContext ctx, Table table) {
        @SuppressWarnings("unchecked")
        Map<String, Long> processedRecordsMap = ctx.getProperty(ImportProperty.PROCESSED_RECORDS, Map.class);

        @SuppressWarnings("unchecked")
        Map<String, Long> lastModifiedTimes = ctx.getProperty(ImportProperty.LAST_MODIFIED_DATE, Map.class);
        processedRecordsMap.put(table.getName(), 0L);
        lastModifiedTimes.put(table.getName(), DateTime.now().getMillis());
    }

    public Properties getProperties(ImportContext ctx, Table table) {
        Properties props = new Properties();
        props.put("errorLineNumber", errorLineNumber + "");
        props.put(CSVFileImportProperty.CSV_FILE_MAPPER_CORES.name(), csvImportMapperCores + "");
        props.put("yarn.mr.hdfs.class.path",
                String.format("/app/%s/eai/lib", versionManager.getCurrentVersionInStack(stackName)));
        props.put(MapReduceProperty.CUSTOMER.name(), ctx.getProperty(ImportProperty.CUSTOMER, String.class));
        props.put(MapReduceProperty.QUEUE.name(), LedpQueueAssigner.getEaiQueueNameForSubmission());
        props.put(MapReduceProperty.OUTPUT.name(), new HdfsUriGenerator().getHdfsUriForSqoop(ctx, table));

        String hdfsFileToImport = ctx.getProperty(ImportProperty.HDFSFILE, String.class);

        props.put("eai.table.schema", JsonUtils.serialize(table));
        String idColumnName = ctx.getProperty(ImportProperty.ID_COLUMN_NAME, String.class);
        if (StringUtils.isEmpty(idColumnName)) {
            props.put("eai.id.column.name", "");
        } else {
            props.put("eai.id.column.name", idColumnName);
        }
        props.put("eai.redis.timeout", String.valueOf(redisTimeout));
        props.put("eai.redis.endpoint", elastiCacheService.getPrimaryEndpointAddress());
        props.put("eai.redis.local", String.valueOf(localRedis));
        props.put("eai.import.use.s3.input", ctx.getProperty(ImportProperty.USE_S3_INPUT, String.class, "false"));
        props.put("eai.import.aws.s3.bucket", ctx.getProperty(ImportProperty.S3_BUCKET, String.class, ""));
        props.put("eai.import.aws.s3.object.key", ctx.getProperty(ImportProperty.S3_OBJECT_KEY, String.class, ""));
        props.put("eai.import.aws.s3.file.size", ctx.getProperty(ImportProperty.S3_FILE_SIZE, String.class, "0"));
        props.put("eai.import.detail.error", ctx.getProperty(ImportProperty.NEED_DETAIL_ERROR, String.class, "false"));
        props.put("eai.import.aws.region", awsRegion);
        props.put("eai.import.aws.access.key", CipherUtils.encrypt(awsAccessKey));
        props.put("eai.import.aws.secret.key", CipherUtils.encrypt(awsSecretKey));
        props.put(MapReduceProperty.INPUT.name(), hdfsFileToImport);
        List<String> cacheFiles;
        try {
            cacheFiles = EaiJobUtil.getCacheFiles(ctx.getProperty(ExportProperty.HADOOPCONFIG, Configuration.class),
                    versionManager.getCurrentVersionInStack(stackName));
            cacheFiles.add(hdfsFileToImport);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        props.setProperty(MapReduceProperty.CACHE_FILE_PATH.name(), String.join(",", cacheFiles));
        return props;
    }

    @Override
    public Table importMetadata(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        log.info(String.format("Importing metadata for table %s with filter %s", table, filter));

        String metadataFile = ctx.getProperty(ImportProperty.METADATAFILE, String.class);
        String contents;
        // CDL import won't update the attribute name to interface name.
        boolean skipUpdate = Boolean.parseBoolean(ctx.getProperty(ImportProperty.SKIP_UPDATE_ATTR_NAME, String.class));

        if (metadataFile != null) {
            try {
                contents = FileUtils.readFileToString(new File(metadataFile), "UTF-8");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            contents = ctx.getProperty(ImportProperty.METADATA, String.class);
        }
        ModelingMetadata metadata = JsonUtils.deserialize(contents, ModelingMetadata.class);

        Map<String, ModelingMetadata.AttributeMetadata> attrMap = new HashMap<>();
        for (ModelingMetadata.AttributeMetadata attr : metadata.getAttributeMetadata()) {
            attrMap.put(attr.getColumnName(), attr);
        }

        for (Attribute attr : table.getAttributes()) {
            ModelingMetadata.AttributeMetadata attrMetadata = attrMap.get(attr.getName());
            if (!skipUpdate && attr.getInterfaceName() != null) {
                if (attr.getName().equals(InterfaceName.Id.name())) {
                    ctx.setProperty(ImportProperty.ID_COLUMN_NAME, attr.getInterfaceName().name());
                }
                attr.setName(attr.getInterfaceName().name());
            }
            if (attrMetadata != null && attr.getSourceLogicalDataType() == null) {
                attr.setSourceLogicalDataType(attrMetadata.getDataType());
            } else if (attrMetadata == null) {
                throw new LedpException(LedpCode.LEDP_17002, new String[]{attr.getName()});
            }
        }
        if (table.getAttribute(InterfaceName.InternalId.name()) == null) {
            addInternalId(table);
        } else {
            table.getAttribute(InterfaceName.InternalId.name()).setNullable(true);
        }
        List<Attribute> defaultAttrs = getDefaultAttribute(ctx);
        if (CollectionUtils.isNotEmpty(defaultAttrs)) {
            table.addAttributes(defaultAttrs);
        }
        return table;
    }

    private List<Attribute> getDefaultAttribute(ImportContext ctx) {
        String defaultMapStr = ctx.getProperty(ImportProperty.DEFAULT_COLUMN_MAP, String.class, null);
        if (StringUtils.isNotEmpty(defaultMapStr)) {
            Map<?, ?> rawMap = JsonUtils.deserialize(defaultMapStr, Map.class);
            if (rawMap != null) {
                Map<String, String> defaultMap = JsonUtils.convertMap(rawMap, String.class, String.class);
                List<Attribute> attrList = new ArrayList<>();
                defaultMap.forEach((name, value) -> attrList.add(getDefaultStringAttribute(name, value)));
                return attrList;
            }
        }
        return Collections.emptyList();
    }

    private Attribute getDefaultStringAttribute(String attrName, String value) {
        Attribute attribute = new Attribute();
        attribute.setName(attrName);
        attribute.setDisplayName(attrName);
        attribute.setPhysicalDataType(Type.STRING.getName());
        attribute.setSourceLogicalDataType("");
        attribute.setLogicalDataType("");
        attribute.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
        attribute.setDefaultValueStr(value);
        attribute.setNullable(true);
        return attribute;
    }

    private void addInternalId(Table table) {
        Attribute internalId = new Attribute();
        internalId.setName(InterfaceName.InternalId.name());
        internalId.setDisplayName(internalId.getName());
        internalId.setPhysicalDataType(Type.LONG.getName());
        internalId.setSourceLogicalDataType("");
        internalId.setLogicalDataType(LogicalDataType.InternalId);
        internalId.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
        internalId.setNullable(true);
        table.addAttribute(internalId);
    }

    @Override
    public ImportContext resolveFilterExpression(String expression, ImportContext ctx) {
        return ctx;
    }

    @Override
    protected AvroTypeConverter getAvroTypeConverter() {
        return null;
    }
}
