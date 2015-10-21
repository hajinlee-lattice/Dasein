package com.latticeengines.eai.service.impl;

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
        List<SourceImportConfiguration> sourceImportConfigs = importConfig.getSourceConfigurations();
        String customer = importConfig.getCustomer();
        context.setProperty(ImportProperty.CUSTOMER, customer);
        String targetPath = createTargetPath(importConfig.getCustomer());
        context.setProperty(ImportProperty.TARGETPATH, targetPath);
        context.setProperty(ImportProperty.EXTRACT_PATH, new HashMap<String, String>());
        context.setProperty(ImportProperty.LAST_MODIFIED_DATE, new HashMap<String, Long>());
        List<Table> tableMetadata = null;
        for (SourceImportConfiguration sourceImportConfig : sourceImportConfigs) {
            log.info("Importing for " + sourceImportConfig.getSourceType());
            Map<String, String> props = sourceImportConfig.getProperties();
            log.info("Moving properties from import config to import context.");
            for (Map.Entry<String, String> entry : props.entrySet()) {
                log.info("Property " + entry.getKey() + " = " + entry.getValue());
                context.setProperty(entry.getKey(), entry.getValue());
            }

            ImportService importService = ImportService.getImportService(sourceImportConfig.getSourceType());
            tableMetadata = importService.importMetadata(sourceImportConfig, context);

            sourceImportConfig.setTables(tableMetadata);
            for (Table table : tableMetadata) {
                LastModifiedKey lmd = eaiMetadataService.getLastModifiedKey(CustomerSpace.parse(customer).toString(),
                        table);
                StringBuilder filter = new StringBuilder();
                String lastModifiedDate;
                DateTime date;
                if (lmd != null) {
                    lastModifiedDate = lmd.getAttributeNames()[0];
                    date = new DateTime(lmd.getLastModifiedTimestamp());
                } else {
                    lastModifiedDate = "LastModifiedDate";
                    date = new DateTime(1000000000000L);
                }
                String defaultFilter = sourceImportConfig.getFilter(table.getName());
                if (!StringUtils.isEmpty(defaultFilter)) {
                    filter.append(defaultFilter).append(", ");
                }
                filter.append(lastModifiedDate).append(" >= ").append(date).append(" Order By ")
                        .append(lastModifiedDate).append(" Desc ").toString();

                sourceImportConfig.setFilter(table.getName(), filter.toString());
            }

            importService.importDataAndWriteToHdfs(sourceImportConfig, context);

        }
        return tableMetadata;
    }

    @Override
    public ApplicationId submitExtractAndImportJob(ImportConfiguration importConfig) {
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
            extractAndImport(importConfig, importContext);
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

        String customer = importConfig.getCustomer();

        eaiJob.setClient("eaiClient");
        eaiJob.setCustomer(customer);

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), customer);
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

    public String createTargetPath(String customer) {
        CustomerSpace space = CustomerSpace.parse(customer);
        return PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), space).toString();
    }

    public void cleanUpTargetPathData(ImportContext context) throws Exception {
        @SuppressWarnings("unchecked")
        Map<String, String> targetPaths = context.getProperty(ImportProperty.EXTRACT_PATH, Map.class);
        for (Map.Entry<String, String> entry : targetPaths.entrySet()) {
            log.info("Table is： " + entry.getKey() + "  Path is: " + entry.getValue());
            HdfsUtils.rmdir(yarnConfiguration, StringUtils.substringBeforeLast(entry.getValue(), "/"));
        }
    }

    @VisibleForTesting
    void setEaiMetadataService(EaiMetadataService eaiMetadataService) {
        this.eaiMetadataService = eaiMetadataService;
    }
}
