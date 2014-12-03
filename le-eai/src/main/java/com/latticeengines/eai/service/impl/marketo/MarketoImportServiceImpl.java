package com.latticeengines.eai.service.impl.marketo;

import java.util.ArrayList;
import java.util.List;

import org.apache.camel.ProducerTemplate;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.eai.Table;
import com.latticeengines.eai.routes.strategy.ImportStrategy;
import com.latticeengines.eai.routes.strategy.marketo.MarketoImportStrategyBase;
import com.latticeengines.eai.service.ImportService;

@Component("marketoImportService")
public class MarketoImportServiceImpl extends ImportService {
    private static final Log log = LogFactory.getLog(MarketoImportServiceImpl.class);

    @Autowired
    private ProducerTemplate producer;

    public MarketoImportServiceImpl() {
        super(SourceType.MARKETO);
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
    public List<Table> importMetadata(SourceImportConfiguration srcImportConfig, ImportContext ctx) {
        setupAccessToken(ctx);
        List<Table> tablesWithMetadata = new ArrayList<>();
        List<Table> tables = srcImportConfig.getTables();
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
    public void importDataAndWriteToHdfs(SourceImportConfiguration srcImportConfig, ImportContext ctx) {
        setupAccessToken(ctx);
        List<Table> tables = srcImportConfig.getTables();
        for (Table table : tables) {
            MarketoImportStrategyBase strategy = (MarketoImportStrategyBase) ImportStrategy.getImportStrategy(SourceType.MARKETO, table);
            if (strategy == null) {
                log.error("No import strategy for Marketo table " + table.getName());
                continue;
            }
            strategy.importData(producer, table, srcImportConfig.getFilter(table.getName()), ctx);
        }
    }

}
