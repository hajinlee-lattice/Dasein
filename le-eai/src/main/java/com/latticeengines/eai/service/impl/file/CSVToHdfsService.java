package com.latticeengines.eai.service.impl.file;

import static com.latticeengines.eai.util.HdfsUriGenerator.EXTRACT_DATE_FORMAT;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.ConnectorConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.eai.runtime.service.EaiRuntimeService;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

@Component("csvToHdfsService")
public class CSVToHdfsService extends EaiRuntimeService<CSVToHdfsConfiguration> {

    private static Logger log = LoggerFactory.getLogger(CSVToHdfsService.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private EaiMetadataService eaiMetadataService;

    @Override
    public void invoke(CSVToHdfsConfiguration config) {
        List<SourceImportConfiguration> sourceImportConfigs = config.getSourceConfigurations();
        String customerSpace = config.getCustomerSpace().toString();

        ImportContext context = new ImportContext(yarnConfiguration);
        context.setProperty(ImportProperty.CUSTOMER, customerSpace);
        context.setProperty(ImportProperty.CUSTOMER, customerSpace);

        context.setProperty(ImportProperty.EXTRACT_PATH, new HashMap<String, String>());
        context.setProperty(ImportProperty.PROCESSED_RECORDS, new HashMap<String, Long>());
        context.setProperty(ImportProperty.LAST_MODIFIED_DATE, new HashMap<String, Long>());
        context.setProperty(ImportProperty.HDFSFILE, config.getFilePath());
        context.setProperty(ImportProperty.MULTIPLE_EXTRACT, new HashMap<String, Boolean>());
        context.setProperty(ImportProperty.EXTRACT_PATH_LIST, new HashMap<String, List<String>>());
        context.setProperty(ImportProperty.EXTRACT_RECORDS_LIST, new HashMap<String, List<Long>>());
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, config.getJobIdentifier());
        if (dataFeedTask == null) {
            throw new RuntimeException("Cannot find the dataFeed task for import!");
        }
        Table template = dataFeedTask.getImportTemplate();
        log.info(String.format("Modeling metadata for template: %s", JsonUtils.serialize(template.getModelingMetadata
                ())));
        context.setProperty(ImportProperty.METADATA, JsonUtils.serialize(template.getModelingMetadata()));
        String targetPath = createTargetPath(config.getCustomerSpace());
        List<Table> tableMetadata = new ArrayList<>();
        for (SourceImportConfiguration sourceImportConfig : sourceImportConfigs) {
            log.info("Importing for " + sourceImportConfig.getSourceType());
            context.setProperty(ImportProperty.TARGETPATH, targetPath);
            sourceImportConfig.setTables(Arrays.asList(template));
            Map<String, String> props = sourceImportConfig.getProperties();
            log.info("Moving properties from import config to import context.");
            for (Map.Entry<String, String> entry : props.entrySet()) {
                log.info("Property " + entry.getKey() + " = " + entry.getValue());
                context.setProperty(entry.getKey(), entry.getValue());
            }
            sourceImportConfig.getProperties().put(ImportProperty.METADATA, JsonUtils.serialize(template.getModelingMetadata()));
            sourceImportConfig.getProperties().put(ImportProperty.HDFSFILE, config.getFilePath());
            ImportService importService = ImportService.getImportService(sourceImportConfig.getSourceType());
            ConnectorConfiguration connectorConfiguration = importService.generateConnectorConfiguration("", context);
            List<Table> metadata = importService.importMetadata(sourceImportConfig, context, connectorConfiguration);
            tableMetadata.addAll(metadata);

            sourceImportConfig.setTables(metadata);

            importService.importDataAndWriteToHdfs(sourceImportConfig, context, connectorConfiguration);

            Map<String, List<Extract>> extracts = eaiMetadataService.getExtractsForTable(metadata, context);
            String taskId = config.getJobIdentifier();
            List<Extract> extract = extracts.get(template.getName());
            if (extract != null && extract.size() > 0) {
                dataFeedProxy.registerExtract(config.getCustomerSpace().toString(),
                        taskId, template.getName(), extract.get(0));
            }

        }
    }

    private String createTargetPath(CustomerSpace customerSpace) {
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts/%s",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.FILE.getName(), new SimpleDateFormat(EXTRACT_DATE_FORMAT).format(new Date()));
        return targetPath;
    }


}
