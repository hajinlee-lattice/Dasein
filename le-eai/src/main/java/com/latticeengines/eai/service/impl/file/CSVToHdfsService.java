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
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.ConnectorConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordImportCounter;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.eai.runtime.service.EaiRuntimeService;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

@Component("csvToHdfsService")
public class CSVToHdfsService extends EaiRuntimeService<CSVToHdfsConfiguration> {

    private static Logger log = LoggerFactory.getLogger(CSVToHdfsService.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private JobProxy jobProxy;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private EaiMetadataService eaiMetadataService;

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(CSVToHdfsConfiguration config) {
        List<SourceImportConfiguration> sourceImportConfigs = config.getSourceConfigurations();
        String customerSpace = config.getCustomerSpace().toString();

        ImportContext context = new ImportContext(yarnConfiguration);
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
        initJobDetail(config.getJobIdentifier(), SourceType.FILE);
        Table template = dataFeedTask.getImportTemplate();
        log.info(String.format("Modeling metadata for template: %s",
                JsonUtils.serialize(template.getModelingMetadata())));
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
            sourceImportConfig.getProperties().put(ImportProperty.METADATA,
                    JsonUtils.serialize(template.getModelingMetadata()));
            sourceImportConfig.getProperties().put(ImportProperty.HDFSFILE, config.getFilePath());
            ImportService importService = ImportService.getImportService(sourceImportConfig.getSourceType());
            ConnectorConfiguration connectorConfiguration = importService.generateConnectorConfiguration("", context);
            List<Table> metadata = importService.importMetadata(sourceImportConfig, context, connectorConfiguration);
            tableMetadata.addAll(metadata);

            sourceImportConfig.setTables(metadata);
            for (Table table : metadata) {
                context.getProperty(ImportProperty.MULTIPLE_EXTRACT, Map.class).put(table.getName(), Boolean.FALSE);
            }
            importService.importDataAndWriteToHdfs(sourceImportConfig, context, connectorConfiguration);

            waitAndFinalizeJob(config, context, template.getName());

        }
    }

    @SuppressWarnings("unchecked")
    private void waitAndFinalizeJob(CSVToHdfsConfiguration config, ImportContext context, String templateName) {
        // update csv import processed records.
        ApplicationId appId = context.getProperty(ImportProperty.APPID, ApplicationId.class);

        log.info("Application id is : " + appId.toString());
        waitForAppId(appId.toString());
        Counters counters = jobProxy.getMRJobCounters(appId.toString());
        long processedRecords = counters.getCounter(RecordImportCounter.IMPORTED_RECORDS).getValue();
        log.info(String.format("Processed records: %d", processedRecords));
        Map<String, Long> processedRecordsMap = context.getProperty(ImportProperty.PROCESSED_RECORDS, Map.class);
        processedRecordsMap.put(templateName, processedRecords);

        Map<String, String> targetPathsMap = context.getProperty(ImportProperty.EXTRACT_PATH, Map.class);

        updateJobDetailExtractInfo(config.getJobIdentifier(), templateName,
                Arrays.asList(targetPathsMap.get(templateName)), Arrays.asList(Long.toString(processedRecords)));
    }


    private String createTargetPath(CustomerSpace customerSpace) {
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts/%s",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.FILE.getName(), new SimpleDateFormat(EXTRACT_DATE_FORMAT).format(new Date()));
        return targetPath;
    }

    private void waitForAppId(String appId) {
        log.info(String.format("Waiting for appId: %s", appId));

        JobStatus status;
        int maxTries = 17280; // Wait maximum 24 hours
        int i = 0;
        do {
            status = jobProxy.getJobStatus(appId);
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                // Do nothing for InterruptedException
            }
            i++;

            if (i == maxTries) {
                break;
            }
        } while (!YarnUtils.TERMINAL_STATUS.contains(status.getStatus()));

        if (status.getStatus() != FinalApplicationStatus.SUCCEEDED) {
            throw new LedpException(LedpCode.LEDP_28015, new String[] { appId, status.getStatus().toString() });
        }

    }

}
