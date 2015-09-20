package com.latticeengines.eai.routes;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.camel.Exchange;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.metadata.Table;

public class HdfsUriGenerator {

    public String getHdfsUri(Exchange exchange, Table table, String fileName) throws Exception {
        ImportContext importContext = exchange.getProperty(ImportProperty.IMPORTCTX, ImportContext.class);
        Configuration config = importContext.getProperty(ImportProperty.HADOOPCONFIG, Configuration.class);
        String defaultFS = config.get("fs.defaultFS").replace("hdfs://", "hdfs2://");
        String targetPath = importContext.getProperty(ImportProperty.TARGETPATH, String.class);
        return String.format("%s%s/%s/Extracts/%s/%s", defaultFS, targetPath, table.getName(),
                new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date()), fileName).toString();
    }

}
