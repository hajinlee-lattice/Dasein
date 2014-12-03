package com.latticeengines.eai.exposed.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.Attribute;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.exposed.service.DataExtractionService;
import com.latticeengines.eai.service.ImportService;

@Component("dataExtractionService")
public class DataExtractionServiceImpl implements DataExtractionService {

    private static final Log log = LogFactory.getLog(DataExtractionServiceImpl.class);

    @Override
    public void extractAndImport(ImportConfiguration importConfig, ImportContext context) {
        List<SourceImportConfiguration> sourceImportConfigs = importConfig.getSourceConfigurations();
        
        for (SourceImportConfiguration sourceImportConfig : sourceImportConfigs) {
            ImportService importService = ImportService.getImportService(sourceImportConfig.getSourceType());
            List<Table> tableMetadata = importService.importMetadata(sourceImportConfig, context);
            for (Table table : tableMetadata) {
                List<Attribute> attributes = table.getAttributes();

                for (Attribute attribute : attributes) {
                    log.info("Attribute " + attribute.getDisplayName() + " : " + attribute.getPhysicalDataType());
                }
            }
            sourceImportConfig.setTables(tableMetadata);
            importService.importDataAndWriteToHdfs(sourceImportConfig, context);
        }

        
    }

}
