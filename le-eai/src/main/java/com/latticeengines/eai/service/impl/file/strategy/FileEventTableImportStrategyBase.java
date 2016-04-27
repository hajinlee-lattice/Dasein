package com.latticeengines.eai.service.impl.file.strategy;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.avro.Schema.Type;
import org.apache.camel.ProducerTemplate;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.relique.jdbc.csv.CsvDriver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.util.TableUtils;
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

        DbCreds creds = getCreds(ctx);
        String localFileName = createLocalFileForClassGeneration(ctx, table);
        Properties props = getProperties(ctx, table, localFileName);

        try {
            ApplicationId appId = sqoopSyncJobService.importData(localFileName, //
                    new HdfsUriGenerator().getHdfsUriForSqoop(ctx, table), creds, //
                    LedpQueueAssigner.getEaiQueueNameForSubmission(), //
                    ctx.getProperty(ImportProperty.CUSTOMER, String.class), //
                    Arrays.<String> asList(new String[] { table.getAttributes().get(0).getName() }), //
                    null, //
                    1, //
                    props);
            ctx.setProperty(ImportProperty.APPID, appId);
            updateContextProperties(ctx, table);
        } finally {
            FileUtils.deleteQuietly(new File(localFileName + ".csv"));
            FileUtils.deleteQuietly(new File("." + localFileName + ".csv.crc"));
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
        props.put("errorLineNumber", errorLineNumber + "");
        props.put("importMapperClass", LedpCSVToAvroImportMapper.class.toString());
        props.put("columnTypes", StringUtils.join(types, ","));
        props.put("yarn.mr.hdfs.class.path", String.format("/app/%s/eai/lib", versionManager.getCurrentVersionInStack(stackName)));
        System.out.println(TableUtils.createSchema(table.getName(), table).toString());
        props.put("avro.schema", TableUtils.createSchema(table.getName(), table).toString());

        String hdfsFileToImport = ctx.getProperty(ImportProperty.HDFSFILE, String.class);
        props.put("yarn.mr.hdfs.resources", createLatticeCSVFilePath(hdfsFileToImport, localFileName));
        props.put("eai.table.schema", JsonUtils.serialize(table));
        return props;
    }

    private String createLocalFileForClassGeneration(ImportContext context, Table table) {
        String hdfsFileToImport = context.getProperty(ImportProperty.HDFSFILE, String.class);

        CSVParser parser = null;
        CSVPrinter printer = null;
        try {
            String[] tokens = hdfsFileToImport.split("/");
            String[] fileNameTokens = tokens[tokens.length - 1].split("\\.");
            String fileName = fileNameTokens[fileNameTokens.length - 2] + //
                    "-" + UUID.randomUUID() + "_Lattice";
            fileName = fileName.replaceAll("-", "_");

            InputStream stream = HdfsUtils.getInputStream(yarnConfiguration, hdfsFileToImport);
            InputStreamReader reader = new InputStreamReader(stream);
            CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
            parser = new CSVParser(reader, format);

            List<String> headers = new ArrayList<>();
            for (String header : parser.getHeaderMap().keySet()) {
                for (Attribute attr : table.getAttributes()) {
                    if (attr.getDisplayName().equals(header)) {
                        headers.add(attr.getName());
                        break;
                    }
                }
            }

            printer = new CSVPrinter(new FileWriter(fileName + ".csv"), CSVFormat.RFC4180.withDelimiter(',')
                    .withHeader(headers.toArray(new String[] {})));

            for (CSVRecord record : parser.getRecords()) {
                printer.printRecord(record);
            }
            printer.flush();
            HdfsUtils.copyFromLocalToHdfs(yarnConfiguration, fileName + ".csv",
                    createLatticeCSVFilePath(hdfsFileToImport, fileName));
            return fileName;
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18068, e, new String[] { hdfsFileToImport });
        } finally {
            try {
                parser.close();
                printer.close();
            } catch (IOException e) {
                throw new LedpException(LedpCode.LEDP_00002, e);
            }

        }
    }

    private String createLatticeCSVFilePath(String hdfsFileToImport, String fileName) {
        return StringUtils.substringBeforeLast(hdfsFileToImport, "/") + "/" + fileName + ".csv";
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
