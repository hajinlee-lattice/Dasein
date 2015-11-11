package com.latticeengines.eai.service.impl.salesforce;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.eai.service.impl.ImportStrategy;

@Component("salesforceImportService")
public class SalesforceImportServiceImpl extends ImportService {

    public SalesforceImportServiceImpl() {
        super(SourceType.SALESFORCE);
    }

    @Override
    public List<Table> importMetadata(SourceImportConfiguration srcImportConfig, ImportContext ctx) {
        List<Table> newTables = new ArrayList<>();
        List<Table> tables = srcImportConfig.getTables();
        ImportStrategy strategy = ImportStrategy.getImportStrategy(SourceType.SALESFORCE, "AllTables");
        for (Table table : tables) {
            Table newTable = strategy.importMetadata(getProducerTemplate(ctx), table,
                    srcImportConfig.getFilter(table.getName()), ctx);
            newTables.add(newTable);
        }
        return newTables;
    }

    @Override
    public void importDataAndWriteToHdfs(SourceImportConfiguration srcImportConfig, ImportContext ctx) {
        List<Table> tables = srcImportConfig.getTables();
        ImportStrategy strategy = ImportStrategy.getImportStrategy(SourceType.SALESFORCE, "AllTables");
        for (Table table : tables) {
            String filter = srcImportConfig.getFilter(table.getName());
            strategy.importData(getProducerTemplate(ctx), table, filter, ctx);
        }
    }

    @Override
    public void updateTableSchema(List<Table> tableMetadata) {
        for (Table table : tableMetadata) {
            for (Attribute attr : table.getAttributes()) {
                if (StringUtils.isNotEmpty(attr.getSemanticType())) {
                    attr.setName(attr.getSemanticType());
                }
            }
        }
    }

}
