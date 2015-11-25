package com.latticeengines.eai.service.impl.file;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.eai.service.impl.ImportStrategy;

@Component("fileImportService")
public class FileImportServiceImpl extends ImportService {

    public FileImportServiceImpl() {
        super(SourceType.FILE);
    }

    @Override
    public List<Table> importMetadata(SourceImportConfiguration srcImportConfig, ImportContext context) {
        List<Table> newTables = new ArrayList<>();
        List<Table> tables = srcImportConfig.getTables();
        ImportStrategy strategy = ImportStrategy.getImportStrategy(SourceType.FILE, "EventTable");
        context.setProperty(ImportProperty.METADATAFILE, //
                srcImportConfig.getProperties().get(ImportProperty.METADATAFILE));
        for (Table table : tables) {
            Table newTable = strategy.importMetadata(null, table, null, context);
            newTables.add(newTable);
        }
        return newTables;
    }

    @Override
    public void importDataAndWriteToHdfs(SourceImportConfiguration srcImportConfig, ImportContext context) {
        context.setProperty(ImportProperty.DATAFILEDIR, //
                srcImportConfig.getProperties().get(ImportProperty.DATAFILEDIR));
        context.setProperty(ImportProperty.METADATAFILE, //
                srcImportConfig.getProperties().get(ImportProperty.METADATAFILE));
        context.setProperty(ImportProperty.FILEURLPROPERTIES, //
                srcImportConfig.getProperties().get(ImportProperty.FILEURLPROPERTIES));

        ImportStrategy strategy = ImportStrategy.getImportStrategy(SourceType.FILE, "EventTable");
        List<Table> tables = srcImportConfig.getTables();
        for (Table table : tables) {
            strategy.importData(null, table, null, context);
        }

    }

    @Override
    public void validate(SourceImportConfiguration extractionConfig, ImportContext context) {
        super.validate(extractionConfig, context);

        if (extractionConfig.getTables().size() > 1) {
            throw new LedpException(LedpCode.LEDP_17001);
        }
    }
}
