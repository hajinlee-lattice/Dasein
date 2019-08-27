package com.latticeengines.eai.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Table;

public class HdfsUriGenerator {

    public static final String EXTRACT_DATE_FORMAT = "yyyy-MM-dd-HH-mm-ss.SSS";

    @SuppressWarnings("unchecked")
    public String getHdfsUriForCamel(Exchange exchange, Table table, String fileName) {
        ImportContext importContext = exchange.getProperty(ImportProperty.IMPORTCTX, ImportContext.class);
        try {
            Configuration config = importContext.getProperty(ImportProperty.HADOOPCONFIG, Configuration.class);
            String defaultFS = config.get("fs.defaultFS").replace("hdfs://", "hdfs2://");

            String targetPath = importContext.getProperty(ImportProperty.TARGETPATH, String.class);
            // store extract time stamp folder
            String hdfsUri = String.format("%s%s/%s/Extracts/%s/%s", defaultFS, targetPath, table.getName(),
                    new SimpleDateFormat(EXTRACT_DATE_FORMAT).format(new Date()), fileName).toString();
            importContext.getProperty(ImportProperty.EXTRACT_PATH, Map.class).put(table.getName(),
                    hdfsUri.replace("hdfs2", "hdfs"));
            return hdfsUri;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public String getHdfsUriForSqoop(ImportContext importContext, Table table) {
        try {
            Configuration config = importContext.getProperty(ImportProperty.HADOOPCONFIG, Configuration.class);
            String defaultFS = config.get("fs.defaultFS");
            String targetPath = importContext.getProperty(ImportProperty.TARGETPATH, String.class);
            // store extract time stamp folder
            String hdfsUri = String.format("%s%s/%s/Extracts/%s", defaultFS, targetPath, table.getName(),
                    new SimpleDateFormat(EXTRACT_DATE_FORMAT).format(new Date())).toString();
            importContext.getProperty(ImportProperty.EXTRACT_PATH, Map.class).put(table.getName(), hdfsUri + "/*.avro");
            return hdfsUri;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
