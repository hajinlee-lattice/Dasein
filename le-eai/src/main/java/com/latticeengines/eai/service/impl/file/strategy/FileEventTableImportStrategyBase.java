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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.eai.service.impl.AvroTypeConverter;
import com.latticeengines.eai.service.impl.ImportStrategy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("fileEventTableImportStrategyBase")
public class FileEventTableImportStrategyBase extends ImportStrategy {

    @Autowired
    private SqoopSyncJobService sqoopSyncJobService;
    
    @Autowired
    private Configuration yarnConfiguration;

    public FileEventTableImportStrategyBase() {
        this("File.EventTable");
    }

    public FileEventTableImportStrategyBase(String key) {
        super(key);
    }

    @Override
    public void importData(ProducerTemplate template, Table table, String filter, ImportContext ctx) {
        DbCreds creds = getCreds(ctx);
        Properties props = getProperties(ctx, table);
        
        try {
            ApplicationId appId = sqoopSyncJobService.importData(table.getName(), //
                    ctx.getProperty(ImportProperty.TARGETPATH, String.class), //
                    creds, //
                    LedpQueueAssigner.getPropDataQueueNameForSubmission(), //
                    ctx.getProperty(ImportProperty.CUSTOMER, String.class), //
                    Arrays.<String> asList(new String[] { table.getAttributes().get(0).getName() }), //
                    null, //
                    1, //
                    props);
            ctx.setProperty(ImportProperty.APPID, appId);
        } finally {
            FileUtils.deleteQuietly(new File(table.getName() + ".csv"));
            FileUtils.deleteQuietly(new File("." + table.getName() + ".csv.crc"));
        }
    }
    
    private DbCreds getCreds(ImportContext ctx) {
        String url = createJdbcUrl(ctx);
        String driver = "org.relique.jdbc.csv.CsvDriver";
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(url).driverClass(driver);
        return new DbCreds(builder);
    }
    
    private Properties getProperties(ImportContext ctx, Table table) {
        List<String> types = new ArrayList<>();
        for (Attribute attr : table.getAttributes()) {
            types.add(attr.getPhysicalDataType());
        }

        Properties props = new Properties();
        props.put("columnTypes", StringUtils.join(types, ","));
        props.put("yarn.mr.hdfs.class.path", "/app/eai/lib");
        
        String hdfsFileToImport = ctx.getProperty(ImportProperty.HDFSFILE, String.class);
        String fileName = createLocalFileForClassGeneration(hdfsFileToImport);
        table.setName(fileName);
        props.put("yarn.mr.hdfs.resources", String.format("%s#%s.csv", hdfsFileToImport, fileName));

        return props;
    }
    
    private String createLocalFileForClassGeneration(String hdfsFileToImport) {
        try {
            String[] tokens = hdfsFileToImport.split("/");
            String[] fileNameTokens = tokens[tokens.length-1].split("\\.");
            String fileName = fileNameTokens[fileNameTokens.length-2] + //
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
        String metadataFile = ctx.getProperty(ImportProperty.METADATAFILE, String.class);
        String contents;
        try {
            contents = FileUtils.readFileToString(new File(metadataFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ModelingMetadata metadata = JsonUtils.deserialize(contents, ModelingMetadata.class);

        Map<String, ModelingMetadata.AttributeMetadata> attrMap = new HashMap<>();
        for (ModelingMetadata.AttributeMetadata attr : metadata.getAttributeMetadata()) {
            attrMap.put(attr.getColumnName(), attr);
        }

        for (Attribute attr : table.getAttributes()) {
            ModelingMetadata.AttributeMetadata attrMetadata = attrMap.get(attr.getName());

            if (attrMetadata != null) {
                attr.setDisplayName(attrMetadata.getDisplayName());
                attr.setPhysicalDataType(attrMetadata.getDataType());
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
            for (Map.Entry<String, String> entry : fileUrlProperties.entrySet()) {
                props.add(String.format("%s=%s", entry.getKey(), entry.getValue()));
            }
            url += "?" + StringUtils.join(props, "&");
        }
        return url;
    }
}
