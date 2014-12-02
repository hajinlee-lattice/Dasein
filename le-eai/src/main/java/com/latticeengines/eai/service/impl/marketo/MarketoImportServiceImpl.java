package com.latticeengines.eai.service.impl.marketo;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.ProducerTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.DataExtractionConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.routes.SourceType;
import com.latticeengines.eai.routes.strategy.ImportStrategy;
import com.latticeengines.eai.routes.strategy.marketo.MarketoImportStrategyBase;
import com.latticeengines.eai.service.ImportService;

@Component("marketoImportService")
public class MarketoImportServiceImpl extends ImportService {
    private static final Log log = LogFactory.getLog(MarketoImportServiceImpl.class);

    @Autowired
    private ProducerTemplate producer;

    public MarketoImportServiceImpl() {
    }
    
    private void setupAccessToken(ImportContext ctx) {
        ImportStrategy accessTokenStrategy = ImportStrategy.getImportStrategy(SourceType.MARKETO, "AccessToken");
        if (accessTokenStrategy == null) {
            throw new RuntimeException("Access token strategy not available.");
        } else {
            accessTokenStrategy.importData(producer, null, null, ctx);
        }
    }

    @Override
    public List<Table> importMetadata(DataExtractionConfiguration extractionConfig, ImportContext ctx) {
        setupAccessToken(ctx);
        List<Table> tablesWithMetadata = new ArrayList<>();
        List<Table> tables = extractionConfig.getTables();
        for (Table table : tables) {
            ImportStrategy strategy = ImportStrategy.getImportStrategy(SourceType.MARKETO, table);
            if (strategy == null) {
                log.error("No import strategy for Marketo table " + table.getName());
                continue;
            }
            tablesWithMetadata.add(strategy.importMetadata(producer, table, ctx));
        }
        return tablesWithMetadata;
    }

    @Override
    public void importDataAndWriteToHdfs(DataExtractionConfiguration extractionConfig, ImportContext ctx) {
        setupAccessToken(ctx);
        List<Table> tables = extractionConfig.getTables();
        for (Table table : tables) {
            MarketoImportStrategyBase strategy = (MarketoImportStrategyBase) ImportStrategy.getImportStrategy(SourceType.MARKETO, table);
            if (strategy == null) {
                log.error("No import strategy for Marketo table " + table.getName());
                continue;
            }
            strategy.importData(producer, table, extractionConfig.getFilter(table.getName()), ctx);
        }
    }

}
