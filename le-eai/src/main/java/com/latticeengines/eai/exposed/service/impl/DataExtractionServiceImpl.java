package com.latticeengines.eai.exposed.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.exposed.service.DataExtractionService;
import com.latticeengines.eai.service.ImportService;

@Component("dataExtractionService")
public class DataExtractionServiceImpl implements DataExtractionService {

    @Autowired
    private ImportService salesforceImportService;
    
    @Override
    public void extractAndImport(List<Table> tables) {
        List<Table> tableMetadata = salesforceImportService.importMetadata(tables);
        salesforceImportService.importData(tables);
    }

    
}
