package com.latticeengines.eai.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.eai.exposed.service.EaiService;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.eai.service.ExportService;

@Component("eaiService")
public class EaiServiceImpl implements EaiService {

    private static final Log log = LogFactory.getLog(EaiServiceImpl.class);

    @Autowired
    private DataExtractionService dataExtractionService;

    @Autowired
    private CamelRouteJobService camelRouteJobService;

    @Autowired
    private JobService jobService;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ApplicationId extractAndImport(ImportConfiguration importConfig) {
        ImportConfiguration.ImportType importType = importConfig.getImportType();
        if (importType == null) {
            importType = ImportConfiguration.ImportType.ImportTable;
        }

        switch (importType) {
        case CamelRoute:
            log.info("Directing extractAndImport job to " + camelRouteJobService.getClass().getSimpleName());
            return camelRouteJobService.submitImportJob(importConfig);
        case ImportTable:
        default:
            log.info("Directing extractAndImport job to " + dataExtractionService.getClass().getSimpleName());
            return dataExtractionService.submitExtractAndImportJob(importConfig);
        }
    }

    @Override
    public ApplicationId exportDataFromHdfs(ExportConfiguration exportConfig) {
        ExportDestination exportDest = exportConfig.getExportDestination();
        ExportService exportService = ExportService.getExportService(exportDest);
        log.info("Starting file based export job.");
        return exportService.submitDataExportJob(exportConfig);
    }

}
