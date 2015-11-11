package com.latticeengines.eai.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.camel.ProducerTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Table;

public abstract class ImportService {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ImportService.class);

    private static Map<SourceType, ImportService> services = new HashMap<>();

    protected ImportService(SourceType type) {
        services.put(type, this);
    }

    public static ImportService getImportService(com.latticeengines.domain.exposed.eai.SourceType sourceType) {
        return services.get(sourceType);
    }

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
    public abstract List<Table> importMetadata(SourceImportConfiguration extractionConfig, ImportContext context);

    public abstract void importDataAndWriteToHdfs(SourceImportConfiguration extractionConfig, ImportContext context);

    public void validate(SourceImportConfiguration extractionConfig, ImportContext context) {
        Configuration config = context.getProperty(ImportProperty.HADOOPCONFIG, Configuration.class);

        assert (config != null);
    }

    protected ProducerTemplate getProducerTemplate(ImportContext context) {
        return context.getProperty(ImportProperty.PRODUCERTEMPLATE, ProducerTemplate.class);
    }

    public abstract void updateTableSchema(List<Table> tableMetadata);

}
