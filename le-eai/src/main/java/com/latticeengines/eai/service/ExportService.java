package com.latticeengines.eai.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.ProducerTemplate;
import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ImportProperty;

public abstract class ExportService {

    private static Map<ExportDestination, ExportService> services = new HashMap<>();

    public ExportService(ExportDestination exportDest) {
        services.put(exportDest, this);
    }

    public static ExportService getExportService(ExportDestination exportDest) {
        return services.get(exportDest);
    }

    public abstract void exportDataFromHdfs(ExportConfiguration exportConfig, ExportContext context);

    public abstract ApplicationId submitDataExportJob(ExportConfiguration exportConfig);

    protected ProducerTemplate getProducerTemplate(ExportContext context) {
        return context.getProperty(ImportProperty.PRODUCERTEMPLATE, ProducerTemplate.class);
    }
}
