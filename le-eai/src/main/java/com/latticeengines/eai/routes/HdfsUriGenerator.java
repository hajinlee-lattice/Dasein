package com.latticeengines.eai.routes;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.camel.Exchange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Table;

public class HdfsUriGenerator {

    @SuppressWarnings("unused")
    private Log log = LogFactory.getLog(HdfsUriGenerator.class);

    @SuppressWarnings("unchecked")
    public String getHdfsUri(Exchange exchange, Table table, String fileName) throws Exception {
        ImportContext importContext = exchange.getProperty(ImportProperty.IMPORTCTX, ImportContext.class);
        Configuration config = importContext.getProperty(ImportProperty.HADOOPCONFIG, Configuration.class);
        String defaultFS = config.get("fs.defaultFS").replace("hdfs://", "hdfs2://");
        String targetPath = importContext.getProperty(ImportProperty.TARGETPATH, String.class);
        String hdfsUri = String.format("%s%s/%s/Extracts/%s/%s", defaultFS, targetPath, table.getName(),
                new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date()), fileName).toString();
        // store extract time stamp folder
        importContext.getProperty(ImportProperty.EXTRACT_PATH, Map.class).put(table.getName(),
                hdfsUri.replace("hdfs2", "hdfs"));
        return hdfsUri;
    }

}
