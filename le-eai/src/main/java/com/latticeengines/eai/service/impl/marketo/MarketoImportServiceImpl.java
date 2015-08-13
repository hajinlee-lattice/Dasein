package com.latticeengines.eai.service.impl.marketo;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.eai.service.impl.ImportStrategy;
import com.latticeengines.eai.service.impl.marketo.strategy.MarketoImportStrategyBase;

@Component("marketoImportService")
public class MarketoImportServiceImpl extends ImportService {
    private static final Log log = LogFactory.getLog(MarketoImportServiceImpl.class);

    public MarketoImportServiceImpl() {
        super(SourceType.MARKETO);
    }
    
    private void setupAccessToken(ImportContext ctx) {
        ImportStrategy accessTokenStrategy = ImportStrategy.getImportStrategy(SourceType.MARKETO, "AccessToken");
        if (accessTokenStrategy == null) {
            throw new RuntimeException("Access token strategy not available.");
        } else {
            accessTokenStrategy.importData(getProducerTemplate(ctx), null, null, ctx);
        }
    }

    @Override
    public List<Table> importMetadata(SourceImportConfiguration srcImportConfig, ImportContext ctx) {
        setupAccessToken(ctx);
        List<Table> tablesWithMetadata = new ArrayList<>();
        List<Table> tables = srcImportConfig.getTables();
        for (Table table : tables) {
            log.info(String.format("Importing metadata for table %s.", table.getName()));
            ImportStrategy strategy = ImportStrategy.getImportStrategy(SourceType.MARKETO, table);
            if (strategy == null) {
                log.error(String.format("No import strategy for Marketo table %s.", table.getName()));
                continue;
            } else {
                log.info(String.format("Import strategy for table %s is %s.", table, strategy.toString()));
            }
            tablesWithMetadata.add(strategy.importMetadata(getProducerTemplate(ctx), table, srcImportConfig.getFilter(table.getName()), ctx));
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
            strategy.importData(getProducerTemplate(ctx), table, srcImportConfig.getFilter(table.getName()), ctx);
        }
    }

}
