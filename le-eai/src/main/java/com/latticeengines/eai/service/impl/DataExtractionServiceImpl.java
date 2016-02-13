package com.latticeengines.eai.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.EaiJob;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("dataExtractionService")
public class DataExtractionServiceImpl implements DataExtractionService {

    private static final Log log = LogFactory.getLog(DataExtractionServiceImpl.class);

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private JobService jobService;

    @Autowired
    private ImportContext importContext;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private EaiMetadataService eaiMetadataService;

    @Override
    public List<Table> extractAndImport(ImportConfiguration importConfig, ImportContext context) {
        String metadataUrl = context.getProperty(ImportProperty.METADATAURL, String.class);

        if (metadataUrl != null) {
            eaiMetadataService.setMetadataUrl(metadataUrl);
        }

        List<SourceImportConfiguration> sourceImportConfigs = importConfig.getSourceConfigurations();
        String customerSpace = importConfig.getCustomerSpace().toString();
        context.setProperty(ImportProperty.CUSTOMER, customerSpace);

        context.setProperty(ImportProperty.EXTRACT_PATH, new HashMap<String, String>());
        context.setProperty(ImportProperty.PROCESSED_RECORDS, new HashMap<String, Long>());
        context.setProperty(ImportProperty.LAST_MODIFIED_DATE, new HashMap<String, Long>());
        String targetPath = createTargetPath(customerSpace);
        List<Table> tableMetadata = new ArrayList<>();
        for (SourceImportConfiguration sourceImportConfig : sourceImportConfigs) {
            log.info("Importing for " + sourceImportConfig.getSourceType());
            context.setProperty(ImportProperty.TARGETPATH, targetPath + "/"
                    + sourceImportConfig.getSourceType().getName());

            Map<String, String> props = sourceImportConfig.getProperties();
            log.info("Moving properties from import config to import context.");
            for (Map.Entry<String, String> entry : props.entrySet()) {
                log.info("Property " + entry.getKey() + " = " + entry.getValue());
                context.setProperty(entry.getKey(), entry.getValue());
            }

            if (sourceImportConfig.getSourceType() != SourceType.FILE) {
                List<Table> importTables = eaiMetadataService.getImportTables( //
                        importConfig.getCustomerSpace().toString());
                sourceImportConfig.setTables(importTables);
            }

            ImportService importService = ImportService.getImportService(sourceImportConfig.getSourceType());
            List<Table> metadata = importService.importMetadata(sourceImportConfig, context);
            tableMetadata.addAll(metadata);

            sourceImportConfig.setTables(metadata);

            setFilters(sourceImportConfig, customerSpace);
            importService.importDataAndWriteToHdfs(sourceImportConfig, context);

        }
        return tableMetadata;
    }

    @VisibleForTesting
    void setFilters(SourceImportConfiguration sourceImportConfig, String customerSpace) {
        List<Table> tableMetadata = sourceImportConfig.getTables();
        Map<String, String> filters = sourceImportConfig.getFilters();
        if (filters.isEmpty() && sourceImportConfig.getSourceType() == SourceType.FILE) {
            return;
        }
        for (Table table : tableMetadata) {
            LastModifiedKey lmk = eaiMetadataService.getLastModifiedKey(customerSpace, table);
            StringBuilder filter = new StringBuilder();
            String lastModifiedKey;
            DateTime date;
            if (lmk != null) {
                lastModifiedKey = lmk.getAttributeNames()[0];
                log.info("Table: " + table.getName() + ", LastModifiedTimeStamp: " + lmk.getLastModifiedTimestamp());
                date = new DateTime(lmk.getLastModifiedTimestamp());
            } else {
                throw new LedpException(LedpCode.LEDP_17006, new String[] { customerSpace });
            }
            String defaultFilter = sourceImportConfig.getFilter(table.getName());
            if (!StringUtils.isEmpty(defaultFilter)) {
                filter.append(defaultFilter).append(", ");
            }
            filter.append(lastModifiedKey).append(" >= ").append(date).append(" Order By ").append(lastModifiedKey)
                    .append(" Desc ").toString();

            sourceImportConfig.setFilter(table.getName(), filter.toString());
        }
    }

    @Override
    public ApplicationId submitExtractAndImportJob(ImportConfiguration importConfig) {
        importContext.setProperty(ImportProperty.CUSTOMER, importConfig.getCustomerSpace().toString());

        ApplicationId appId = null;
        boolean hasNonEaiJobSourceType = false;
        for (SourceImportConfiguration sourceImportConfig : importConfig.getSourceConfigurations()) {
            ImportService importService = ImportService.getImportService(sourceImportConfig.getSourceType());
            importService.validate(sourceImportConfig, importContext);
            if (!sourceImportConfig.getSourceType().willSubmitEaiJob()) {
                hasNonEaiJobSourceType = true;
            }
        }
        if (hasNonEaiJobSourceType) {
            List<Table> tables = extractAndImport(importConfig, importContext);
            eaiMetadataService.updateTableSchema(tables, importContext);
            eaiMetadataService.registerTables(tables, importContext);
            return importContext.getProperty(ImportProperty.APPID, ApplicationId.class);
        } else {
            EaiJob eaiJob = createJob(importConfig);
            appId = jobService.submitJob(eaiJob);
            eaiJob.setId(appId.toString());
            jobEntityMgr.create(eaiJob);
        }
        return appId;
    }

    private EaiJob createJob(ImportConfiguration importConfig) {
        EaiJob eaiJob = new EaiJob();
        String customerSpace = importConfig.getCustomerSpace().toString();

        eaiJob.setClient("eaiClient");
        eaiJob.setCustomer(customerSpace);

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), customerSpace);
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getPropDataQueueNameForSubmission());

        Properties containerProperties = new Properties();
        containerProperties.put(ImportProperty.EAICONFIG, importConfig.toString());
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "128");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");

        eaiJob.setAppMasterPropertiesObject(appMasterProperties);
        eaiJob.setContainerPropertiesObject(containerProperties);
        return eaiJob;
    }

    public String createTargetPath(String customerSpace) {
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        return PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), space).toString();
    }

    public void cleanUpTargetPathData(ImportContext context) throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, String> targetPaths = context.getProperty(ImportProperty.EXTRACT_PATH, Map.class);

        if (targetPaths != null) {
            for (Map.Entry<String, String> entry : targetPaths.entrySet()) {
                log.info("Table is： " + entry.getKey() + " Path is: " + entry.getValue());
                HdfsUtils.rmdir(yarnConfiguration, StringUtils.substringBeforeLast(entry.getValue(), "/"));
            }
        }
    }

    @VisibleForTesting
    void setEaiMetadataService(EaiMetadataService eaiMetadataService) {
        this.eaiMetadataService = eaiMetadataService;
    }
}
