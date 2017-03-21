package com.latticeengines.eai.yarn.runtime;

import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.*;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.ImportService;
import org.apache.hadoop.conf.Configuration;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;

@Component("importVdbTableProcessor")
public class ImportVdbTableProcessor extends SingleContainerYarnProcessor<ImportConfiguration> implements
        ItemProcessor<ImportConfiguration, String> {

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private EaiMetadataService eaiMetadataService;

    @Override
    public String process(ImportConfiguration importConfig) throws Exception {

        SourceImportConfiguration sourceImportConfiguration = importConfig.getSourceConfigurations().get(0);
        ImportService importService = ImportService.getImportService(sourceImportConfiguration.getSourceType());
        ImportContext importContext = new ImportContext(yarnConfiguration);
        //set Import context;
        importContext.setProperty(ImportVdbProperty.DATA_CATEGORY,
                importConfig.getProperty(ImportVdbProperty.DATA_CATEGORY));
        importContext.setProperty(ImportVdbProperty.MERGE_RULE,
                importConfig.getProperty(ImportVdbProperty.MERGE_RULE));
        importContext.setProperty(ImportVdbProperty.QUERY_DATA_ENDPOINT,
                importConfig.getProperty(ImportVdbProperty.QUERY_DATA_ENDPOINT));
        importContext.setProperty(ImportVdbProperty.REPORT_STATUS_ENDPOINT,
                importConfig.getProperty(ImportVdbProperty.REPORT_STATUS_ENDPOINT));
        importContext.setProperty(ImportVdbProperty.TOTAL_ROWS,
                importConfig.getProperty(ImportVdbProperty.TOTAL_ROWS));
        importContext.setProperty(ImportVdbProperty.BATCH_SIZE,
                importConfig.getProperty(ImportVdbProperty.BATCH_SIZE));
        importContext.setProperty(ImportVdbProperty.VDB_QUERY_HANDLE,
                importConfig.getProperty(ImportVdbProperty.VDB_QUERY_HANDLE));
        importContext.setProperty(ImportVdbProperty.METADATA_LIST,
                importConfig.getProperty(ImportVdbProperty.METADATA_LIST));
        importContext.setProperty(ImportVdbProperty.TARGETPATH,
                importConfig.getProperty(ImportVdbProperty.TARGETPATH));
        importContext.setProperty(ImportVdbProperty.EXTRACT_NAME,
                importConfig.getProperty(ImportVdbProperty.EXTRACT_NAME));
        importContext.setProperty(ImportVdbProperty.EXTRACT_IDENTIFIER,
                importConfig.getProperty(ImportVdbProperty.EXTRACT_IDENTIFIER));

        String customerSpace = importConfig.getCustomerSpace().toString();
        importContext.setProperty(ImportProperty.CUSTOMER, customerSpace);

        importContext.setProperty(ImportProperty.EXTRACT_PATH, new HashMap<String, String>());
        importContext.setProperty(ImportProperty.PROCESSED_RECORDS, new HashMap<String, Long>());

        List<Table> metadata = importService.importMetadata(sourceImportConfiguration, importContext);


        sourceImportConfiguration.setTables(metadata);

        importService.importDataAndWriteToHdfs(sourceImportConfiguration, importContext);

        eaiMetadataService.updateTableSchema(metadata, importContext);
        eaiMetadataService.registerTables(metadata, importContext);

        return null;
    }

}
