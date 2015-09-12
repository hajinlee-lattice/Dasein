package com.latticeengines.eai.service.impl;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.dataplatform.exposed.yarn.client.AppMasterProperty;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.domain.exposed.eai.EaiJob;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component("dataExtractionService")
public class DataExtractionServiceImpl implements DataExtractionService {

    private static final Log log = LogFactory.getLog(DataExtractionServiceImpl.class);

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private JobService jobService;

    @Override
    public void extractAndImport(ImportConfiguration importConfig, ImportContext context) {
        List<SourceImportConfiguration> sourceImportConfigs = importConfig.getSourceConfigurations();
        context.setProperty(ImportProperty.CUSTOMER, importConfig.getCustomer());

        for (SourceImportConfiguration sourceImportConfig : sourceImportConfigs) {
            log.info("Importing for " + sourceImportConfig.getSourceType());
            Map<String, String> props = sourceImportConfig.getProperties();
            log.info("Moving properties from import config to import context.");
            for (Map.Entry<String, String> entry : props.entrySet()) {
                log.info("Property " + entry.getKey() + " = " + entry.getValue());
                context.setProperty(entry.getKey(), entry.getValue());
            }

            ImportService importService = ImportService.getImportService(sourceImportConfig.getSourceType());
            List<Table> tableMetadata = importService.importMetadata(sourceImportConfig, context);
            for (Table table : tableMetadata) {
                List<Attribute> attributes = table.getAttributes();

                for (Attribute attribute : attributes) {
                    log.info("Attribute " + attribute.getDisplayName() + " : " + attribute.getPhysicalDataType());
                }
            }
            sourceImportConfig.setTables(tableMetadata);

            importService.importDataAndWriteToHdfs(sourceImportConfig, context);
        }
    }

    @Override
    public ApplicationId submitExtractAndImportJob(ImportConfiguration importConfig, ImportContext context) {
        ApplicationId appId = null;

        boolean hasNonEaiJobSourceType = false;
        for (SourceImportConfiguration sourceImportConfig : importConfig.getSourceConfigurations()) {
            ImportService importService = ImportService.getImportService(sourceImportConfig.getSourceType());
            importService.validate(sourceImportConfig, context);
            if (!sourceImportConfig.getSourceType().willSubmitEaiJob()) {
                hasNonEaiJobSourceType = true;
            }
        }
        if (hasNonEaiJobSourceType) {
            extractAndImport(importConfig, context);
            return context.getProperty(ImportProperty.APPID, ApplicationId.class);
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
        String targetPath = importConfig.getTargetPath();
        eaiJob.setClient("eaiClient");
        eaiJob.setCustomer(customer);
        eaiJob.setTargetPath(targetPath);

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), customer);
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getPropDataQueueNameForSubmission());

        Properties containerProperties = new Properties();
        containerProperties.put(ImportProperty.EAICONFIG, importConfig.toString());
        containerProperties.put(ImportProperty.TARGETPATH, targetPath);
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), "128");
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");

        eaiJob.setAppMasterPropertiesObject(appMasterProperties);
        eaiJob.setContainerPropertiesObject(containerProperties);
        return eaiJob;
    }


}
