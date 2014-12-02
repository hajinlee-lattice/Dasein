package com.latticeengines.eai.exposed.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.DataExtractionConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.exposed.service.DataExtractionService;
import com.latticeengines.eai.service.ImportService;

@Component("dataExtractionService")
public class DataExtractionServiceImpl implements DataExtractionService {

    private static final Log log = LogFactory.getLog(DataExtractionServiceImpl.class);

    @Autowired
    private ImportService salesforceImportService;

    @Override
    public void extractAndImport(DataExtractionConfiguration extractionConfig, ImportContext context) {
        List<Table> tables = extractionConfig.getTables();
        List<Table> tableMetadata = salesforceImportService.importMetadata(extractionConfig, context);

        for (Table table : tableMetadata) {
            List<Attribute> attributes = table.getAttributes();

            for (Attribute attribute : attributes) {
                log.info("Attribute " + attribute.getDisplayName() + " : " + attribute.getPhysicalDataType());
            }
        }
        extractionConfig.setTables(tableMetadata);
        salesforceImportService.importDataAndWriteToHdfs(extractionConfig, context);
    }

}
