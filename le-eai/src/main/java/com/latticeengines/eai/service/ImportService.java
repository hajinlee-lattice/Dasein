package com.latticeengines.eai.service;

import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.routes.ImportProperty;

public abstract class ImportService {
    
    private static final Log log = LogFactory.getLog(ImportService.class);

    /**
     * Import metadata from the specific connector. The original list of table
     * metadata will be decorated with the following:
     * 
     * 1. Attribute length, precision, scale, physical type, logical type 2.
     * Avro schema associated with the Table
     * 
     * @param tables
     *            list of tables that only has table name and attribute names
     * @return
     */
    public abstract List<Table> importMetadata(List<Table> tables, ImportContext context);

    public abstract void importDataAndWriteToHdfs(List<Table> tables, ImportContext context);
    
    public void validate(ImportContext context) {
        Configuration config = context.getProperty(ImportProperty.HADOOPCONFIG, Configuration.class);
        String targetPath = context.getProperty(ImportProperty.TARGETPATH, String.class);
        
        assert(config != null);
        assert(targetPath != null);
    }
    
    /**
     * Invoke this method when about to invoke a Camel route to do the import.
     * This method will set the required headers for the framework.
     * 
     * @param headers map that will hold the headers
     * @param table table that will be passed into the import route
     * @param context import context
     */
    protected void setHeaders(Map<String, Object> headers, Table table, ImportContext context) {
        if (headers != null) {
            headers.put(ImportProperty.TABLE, table);
            headers.put(ImportProperty.IMPORTCTX, context);
        } else {
            log.warn("headers should not be null. No headers have been set.");
        }
    }

}
