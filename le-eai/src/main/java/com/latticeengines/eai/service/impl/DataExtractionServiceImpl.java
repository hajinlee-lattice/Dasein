package com.latticeengines.eai.service.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.ConnectorConfiguration;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.LastModifiedKey;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.service.DataExtractionService;
import com.latticeengines.eai.service.EaiImportJobDetailService;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.EaiYarnService;
import com.latticeengines.eai.service.ImportService;

@Component("dataExtractionService")
public class DataExtractionServiceImpl implements DataExtractionService {

    private static final Logger log = LoggerFactory.getLogger(DataExtractionServiceImpl.class);

    @Autowired
    private EaiMetadataService eaiMetadataService;

    @Autowired
    private EaiImportJobDetailService eaiImportJobDetailService;

    @Autowired
    private EaiYarnService eaiYarnService;

    @Autowired
    private Configuration yarnConfiguration;

    @Override
    public List<Table> extractAndImport(ImportConfiguration importConfig, ImportContext context) {

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
            context.setProperty(ImportProperty.TARGETPATH,
                    targetPath + "/" + sourceImportConfig.getSourceType().getName());

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
            ConnectorConfiguration connectorConfiguration = importService.generateConnectorConfiguration("", context);
            List<Table> metadata = importService.importMetadata(sourceImportConfig, context, connectorConfiguration);
            tableMetadata.addAll(metadata);

            sourceImportConfig.setTables(metadata);

            setFilters(sourceImportConfig, customerSpace);
            importService.importDataAndWriteToHdfs(sourceImportConfig, context, connectorConfiguration);

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
        ImportContext importContext = new ImportContext(yarnConfiguration);
        importContext.setProperty(ImportProperty.CUSTOMER, importConfig.getCustomerSpace().toString());

        ApplicationId appId = null;
        boolean hasNonEaiJobSourceType = false;
        for (SourceImportConfiguration sourceImportConfig : importConfig.getSourceConfigurations()) {
            ImportService importService = ImportService.getImportService(sourceImportConfig.getSourceType());
            importService.validate(sourceImportConfig, importContext);
            if (!sourceImportConfig.getSourceType().willSubmitEaiJob()) {
                if (!importConfig.getClass().equals(CSVToHdfsConfiguration.class)) {
                    hasNonEaiJobSourceType = true;
                }
            }
        }
        if (hasNonEaiJobSourceType) {
            List<Table> tables = extractAndImport(importConfig, importContext);
            eaiMetadataService.updateTableSchema(tables, importContext);
            eaiMetadataService.registerTables(tables, importContext);
            return importContext.getProperty(ImportProperty.APPID, ApplicationId.class);
        } else {
            List<EaiImportJobDetail> jobDetails = initailImportJobDetail(importConfig);
            log.info(String.format("Job configuration class before create job: %s", importConfig.getClass().getName()));
            appId = eaiYarnService.submitSingleYarnContainerJob(importConfig);
            if (jobDetails != null) {
                for (EaiImportJobDetail eaiImportJobDetail : jobDetails) {
                    eaiImportJobDetail.setLoadApplicationId(appId.toString());
                    eaiImportJobDetailService.updateImportJobDetail(eaiImportJobDetail);
                }
            }
        }
        return appId;
    }

    private List<EaiImportJobDetail> initailImportJobDetail(ImportConfiguration importConfig) {
        String collectionIdentifiers = importConfig.getProperty(ImportProperty.COLLECTION_IDENTIFIERS);
        if (!StringUtils.isEmpty(collectionIdentifiers)) {
            @SuppressWarnings("unchecked")
            List<Object> identifiersRaw = JsonUtils.deserialize(collectionIdentifiers, List.class);
            List<String> identifiers = JsonUtils.convertList(identifiersRaw, String.class);
            List<EaiImportJobDetail> jobDetails = new ArrayList<>();
            for (String collectionIdentifier : identifiers) {
                EaiImportJobDetail eaiImportJobDetail = eaiImportJobDetailService
                        .getImportJobDetailByCollectionIdentifier(collectionIdentifier);
                if (eaiImportJobDetail != null) {
                    switch (eaiImportJobDetail.getStatus()) {
                    case SUBMITTED:
                        throw new LedpException(LedpCode.LEDP_18136);
                    case RUNNING:
                    case WAITINGREGISTER:
                        throw new LedpException(LedpCode.LEDP_18137,
                                new String[] { eaiImportJobDetail.getLoadApplicationId() });
//                    case SUCCESS:
//                        eaiImportJobDetail.setStatus(ImportStatus.SUBMITTED);
//                        eaiImportJobDetail.setCollectionTimestamp(new Date());
//                        eaiImportJobDetail.setProcessedRecords(0);
//                        eaiImportJobDetail.setSourceType(importConfig.getSourceConfigurations().get(0).getSourceType());
//                        eaiImportJobDetailService.updateImportJobDetail(eaiImportJobDetail);
//                        break;
                    }
                }
                eaiImportJobDetail = new EaiImportJobDetail();
                eaiImportJobDetail.setCollectionIdentifier(collectionIdentifier);
                eaiImportJobDetail.setStatus(ImportStatus.SUBMITTED);
                eaiImportJobDetail.setCollectionTimestamp(new Date());
                eaiImportJobDetail.setProcessedRecords(0);
                eaiImportJobDetail.setSourceType(importConfig.getSourceConfigurations().get(0).getSourceType());
                eaiImportJobDetailService.createImportJobDetail(eaiImportJobDetail);
                jobDetails.add(eaiImportJobDetail);
            }
            return jobDetails;
        } else {
            return null;
        }
    }

    public String createTargetPath(String customerSpace) {
        CustomerSpace space = CustomerSpace.parse(customerSpace);
        return PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), space).toString();
    }

    public void cleanUpTargetPathData(ImportContext context) {
        @SuppressWarnings("unchecked")
        Map<String, String> targetPaths = context.getProperty(ImportProperty.EXTRACT_PATH, Map.class);

        if (targetPaths != null) {
            for (Map.Entry<String, String> entry : targetPaths.entrySet()) {
                log.info("Table is： " + entry.getKey() + " Path is: " + entry.getValue());
                try {
                    HdfsUtils.rmdir(yarnConfiguration, StringUtils.substringBeforeLast(entry.getValue(), "/"));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @VisibleForTesting
    void setEaiMetadataService(EaiMetadataService eaiMetadataService) {
        this.eaiMetadataService = eaiMetadataService;
    }
}
