package com.latticeengines.eai.service.impl.salesforce;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.ProducerTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.routes.strategy.ImportStrategy;
import com.latticeengines.eai.service.ImportService;

@Component("salesforceImportService")
public class SalesforceImportServiceImpl extends ImportService {

    @Autowired
    private ProducerTemplate producer;
    
    public SalesforceImportServiceImpl() {
        super(SourceType.SALESFORCE);
    }

    @Override
    public List<Table> importMetadata(SourceImportConfiguration srcImportConfig, ImportContext ctx) {
        List<Table> newTables = new ArrayList<>();
        List<Table> tables = srcImportConfig.getTables();
        ImportStrategy strategy = ImportStrategy.getImportStrategy(SourceType.SALESFORCE, "AllTables");
        for (Table table : tables) {
            Table newTable = strategy.importMetadata(producer, table, ctx);
            newTables.add(newTable);
        }
        return newTables;
    }

    @Override
    public void importDataAndWriteToHdfs(SourceImportConfiguration extractionConfig, ImportContext ctx) {
        List<Table> tables = extractionConfig.getTables();
        ImportStrategy strategy = ImportStrategy.getImportStrategy(SourceType.SALESFORCE, "AllTables");
        for (Table table : tables) {
            String filter = extractionConfig.getFilter(table.getName());
            strategy.importData(producer, table, filter, ctx);
        }
    }

}
