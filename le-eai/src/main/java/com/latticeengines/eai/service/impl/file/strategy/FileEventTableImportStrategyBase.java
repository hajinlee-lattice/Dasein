package com.latticeengines.eai.service.impl.file.strategy;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema.Type;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import util.HdfsUriGenerator;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.eai.file.runtime.mapreduce.CSVImportJob;
import com.latticeengines.eai.service.impl.AvroTypeConverter;
import com.latticeengines.eai.service.impl.ImportStrategy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("fileEventTableImportStrategyBase")
public class FileEventTableImportStrategyBase extends ImportStrategy {

    private static final Log log = LogFactory.getLog(FileEventTableImportStrategyBase.class);

    @Autowired
    private SqoopSyncJobService sqoopSyncJobService;

    @Autowired
    private JobService jobService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private VersionManager versionManager;

    @Value("${eai.file.csv.error.lines:1000}")
    private int errorLineNumber;

    @Value("${dataplatform.hdfs.stack:}")
    private String stackName;

    public FileEventTableImportStrategyBase() {
        this("File.EventTable");
    }

    public FileEventTableImportStrategyBase(String key) {
        super(key);
    }

    @Override
    public void importData(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        log.info(String.format("Importing data for table %s with filter %s", table, filter));
        Properties props = getProperties(ctx, table, "");

        ApplicationId appId = jobService.submitMRJob(CSVImportJob.CSV_IMPORT_JOB_TYPE, props);
        ctx.setProperty(ImportProperty.APPID, appId);
        updateContextProperties(ctx, table);
    }

    private void updateContextProperties(ImportContext ctx, Table table) {
        @SuppressWarnings("unchecked")
        Map<String, Long> processedRecordsMap = ctx.getProperty(ImportProperty.PROCESSED_RECORDS, Map.class);

        @SuppressWarnings("unchecked")
        Map<String, Long> lastModifiedTimes = ctx.getProperty(ImportProperty.LAST_MODIFIED_DATE, Map.class);
        processedRecordsMap.put(table.getName(), 0L);
        lastModifiedTimes.put(table.getName(), DateTime.now().getMillis());
    }

    private Properties getProperties(ImportContext ctx, Table table, String localFileName) {
        Properties props = new Properties();
        props.put("errorLineNumber", errorLineNumber + "");
        props.put("yarn.mr.hdfs.class.path",
                String.format("/app/%s/eai/lib", versionManager.getCurrentVersionInStack(stackName)));
        props.put(MapReduceProperty.CUSTOMER.name(), ctx.getProperty(ImportProperty.CUSTOMER, String.class));
        props.put(MapReduceProperty.QUEUE.name(), LedpQueueAssigner.getEaiQueueNameForSubmission());
        props.put(MapReduceProperty.OUTPUT.name(), new HdfsUriGenerator().getHdfsUriForSqoop(ctx, table));

        String hdfsFileToImport = ctx.getProperty(ImportProperty.HDFSFILE, String.class);
        props.put(MapReduceProperty.INPUT.name(), hdfsFileToImport);
        props.put("eai.table.schema", JsonUtils.serialize(table));
        return props;
    }

    @Override
    public Table importMetadata(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        log.info(String.format("Importing metadata for table %s with filter %s", table, filter));

        String metadataFile = ctx.getProperty(ImportProperty.METADATAFILE, String.class);
        String contents;

        if (metadataFile != null) {
            try {
                contents = FileUtils.readFileToString(new File(metadataFile));
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
            if (attr.getInterfaceName() != null) {
                attr.setName(attr.getInterfaceName().name());
            }
            if (attrMetadata != null && attr.getSourceLogicalDataType() == null) {
                attr.setSourceLogicalDataType(attrMetadata.getDataType());
            } else if (attrMetadata == null) {
                throw new LedpException(LedpCode.LEDP_17002, new String[] { attr.getName() });
            }
        }
        if (table.getAttribute(InterfaceName.InternalId.name()) == null) {
            addInternalId(table);
        }
        return table;
    }

    private void addInternalId(Table table) {
        Attribute internalId = new Attribute();
        internalId.setName(InterfaceName.InternalId.name());
        internalId.setDisplayName(internalId.getName());
        internalId.setPhysicalDataType(Type.LONG.name());
        internalId.setSourceLogicalDataType("");
        internalId.setLogicalDataType(LogicalDataType.InternalId);
        internalId.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
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
