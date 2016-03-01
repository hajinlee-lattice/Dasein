package com.latticeengines.eai.service.impl.file.strategy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.camel.ProducerTemplate;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.relique.jdbc.csv.CsvDriver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import util.HdfsUriGenerator;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.dataplatform.exposed.sqoop.runtime.mapreduce.LedpCSVToAvroImportMapper;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.eai.exposed.util.AvroSchemaBuilder;
import com.latticeengines.eai.service.impl.AvroTypeConverter;
import com.latticeengines.eai.service.impl.ImportStrategy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("fileEventTableImportStrategyBase")
public class FileEventTableImportStrategyBase extends ImportStrategy {

    private static final Log log = LogFactory.getLog(FileEventTableImportStrategyBase.class);

    @Autowired
    private SqoopSyncJobService sqoopSyncJobService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private VersionManager versionManager;

    public FileEventTableImportStrategyBase() {
        this("File.EventTable");
    }

    public FileEventTableImportStrategyBase(String key) {
        super(key);
    }

    @Override
    public void importData(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        log.info(String.format("Importing data for table %s with filter %s", table, filter));

        DbCreds creds = getCreds(ctx);
        String localFileName = createLocalFileForClassGeneration(ctx);
        Properties props = getProperties(ctx, table, localFileName);

        try {
            ApplicationId appId = sqoopSyncJobService.importData(localFileName, //
                    new HdfsUriGenerator().getHdfsUriForSqoop(ctx, table), creds, //
                    LedpQueueAssigner.getPropDataQueueNameForSubmission(), //
                    ctx.getProperty(ImportProperty.CUSTOMER, String.class), //
                    Arrays.<String> asList(new String[] { table.getAttributes().get(0).getName() }), //
                    null, //
                    1, //
                    props);
            ctx.setProperty(ImportProperty.APPID, appId);
            updateContextProperties(ctx, table);
        } finally {
            FileUtils.deleteQuietly(new File(table.getName() + ".csv"));
            FileUtils.deleteQuietly(new File("." + table.getName() + ".csv.crc"));
        }
    }

    private void updateContextProperties(ImportContext ctx, Table table) {
        @SuppressWarnings("unchecked")
        Map<String, Long> processedRecordsMap = ctx.getProperty(ImportProperty.PROCESSED_RECORDS, Map.class);

        @SuppressWarnings("unchecked")
        Map<String, Long> lastModifiedTimes = ctx.getProperty(ImportProperty.LAST_MODIFIED_DATE, Map.class);
        processedRecordsMap.put(table.getName(), 0L);
        lastModifiedTimes.put(table.getName(), DateTime.now().getMillis());
    }

    private DbCreds getCreds(ImportContext ctx) {
        String url = createJdbcUrl(ctx);
        System.out.println(url);
        String driver = "org.relique.jdbc.csv.CsvDriver";
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(url).driverClass(driver);
        return new DbCreds(builder);
    }

    private Properties getProperties(ImportContext ctx, Table table, String localFileName) {
        List<String> types = new ArrayList<>();
        for (@SuppressWarnings("unused")
        Attribute attr : table.getAttributes()) {
            types.add(String.class.getSimpleName());
        }

        Properties props = new Properties();
        props.put("importType", "File.CSV");
        props.put("importMapperClass", LedpCSVToAvroImportMapper.class.toString());
        props.put("columnTypes", StringUtils.join(types, ","));
        props.put("yarn.mr.hdfs.class.path", String.format("/app/%s/eai/lib", versionManager.getCurrentVersion()));
        System.out.println(AvroSchemaBuilder.createSchema(table.getName(), table).toString());
        props.put("avro.schema", AvroSchemaBuilder.createSchema(table.getName(), table).toString());

        String hdfsFileToImport = ctx.getProperty(ImportProperty.HDFSFILE, String.class);
        props.put("yarn.mr.hdfs.resources", String.format("%s#%s.csv", hdfsFileToImport, localFileName));
        props.put("lattice.eai.file.schema", JsonUtils.serialize(table));
        return props;
    }

    private String createLocalFileForClassGeneration(ImportContext context) {
        String hdfsFileToImport = context.getProperty(ImportProperty.HDFSFILE, String.class);

        try {
            String[] tokens = hdfsFileToImport.split("/");
            String[] fileNameTokens = tokens[tokens.length - 1].split("\\.");
            String fileName = fileNameTokens[fileNameTokens.length - 2] + //
                    "-" + UUID.randomUUID();
            fileName = fileName.replaceAll("-", "_");
            HdfsUtils.copyHdfsToLocal(yarnConfiguration, hdfsFileToImport, //
                    fileName + ".csv");
            return fileName;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18068, e, new String[] { hdfsFileToImport });
        }
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
            ModelingMetadata.AttributeMetadata attrMetadata = null;
            if (attr.getSemanticType() != null) {
                attrMetadata = attrMap.get(attr.getSemanticType().name());
            } else {
                attrMetadata = attrMap.get(attr.getName());
            }

            if (attrMetadata != null) {
                attr.setLogicalDataType(attrMetadata.getDataType());
            } else {
                throw new LedpException(LedpCode.LEDP_17002, new String[] { attr.getName() });
            }
        }
        return table;
    }

    @Override
    public ImportContext resolveFilterExpression(String expression, ImportContext ctx) {
        return ctx;
    }

    @Override
    protected AvroTypeConverter getAvroTypeConverter() {
        return null;
    }

    @SuppressWarnings("unchecked")
    protected String createJdbcUrl(ImportContext ctx) {
        String url = String.format("jdbc:relique:csv:%s", "./");

        String serializedMap = ctx.getProperty(ImportProperty.FILEURLPROPERTIES, String.class);
        Map<String, String> fileUrlProperties = JsonUtils.deserialize(serializedMap, HashMap.class);

        if (fileUrlProperties != null && fileUrlProperties.size() > 0) {
            List<String> props = new ArrayList<>();
            props.add(String.format("%s=%s", CsvDriver.MISSING_VALUE, "''"));
            props.add(String.format("%s=%s", CsvDriver.CHARSET, "UTF-8"));
            for (Map.Entry<String, String> entry : fileUrlProperties.entrySet()) {
                props.add(String.format("%s=%s", entry.getKey(), entry.getValue()));
            }
            url += "?" + StringUtils.join(props, "&");
        }
        return url;
    }
}
