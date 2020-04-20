package com.latticeengines.eai.service.impl.eloqua;

import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ConnectorConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.ImportService;

@Component("eloquaImportService")
public class EloquaImportServiceImpl extends ImportService {

    public EloquaImportServiceImpl() {
        super(SourceType.ELOQUA);
    }

    @Override
    public ConnectorConfiguration generateConnectorConfiguration(String connectorConfig, ImportContext context) {
        return null;
    }

    @Override
    public List<Table> importMetadata(SourceImportConfiguration extractionConfig, ImportContext context,
                                      ConnectorConfiguration connectorConfiguration) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Table> prepareMetadata(List<Table> originalTables, Map<String, String> defaultColumnMap) {
        return null;
    }

    @Override
    public void importDataAndWriteToHdfs(SourceImportConfiguration extractionConfig, ImportContext context,
                                         ConnectorConfiguration connectorConfiguration) {
        // TODO Auto-generated method stub

    }

}
