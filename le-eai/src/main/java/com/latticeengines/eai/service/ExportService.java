package com.latticeengines.eai.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.ProducerTemplate;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ImportProperty;

public abstract class ExportService {

    private static Map<ExportDestination, ExportService> services = new HashMap<>();

    @Autowired
    private EaiYarnService eaiYarnService;

    public ExportService(ExportDestination exportDest) {
        services.put(exportDest, this);
    }

    public static ExportService getExportService(ExportDestination exportDest) {
        return services.get(exportDest);
    }

    public void exportDataFromHdfs(ExportConfiguration exportConfig, ExportContext context) {
        eaiYarnService.submitSingleYarnContainerJob(exportConfig, context);
    }

    protected ProducerTemplate getProducerTemplate(ExportContext context) {
        return context.getProperty(ImportProperty.PRODUCERTEMPLATE, ProducerTemplate.class);
    }
}
