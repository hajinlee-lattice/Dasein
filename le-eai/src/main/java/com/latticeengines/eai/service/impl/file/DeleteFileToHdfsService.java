package com.latticeengines.eai.service.impl.file;

import static com.latticeengines.eai.util.HdfsUriGenerator.EXTRACT_DATE_FORMAT;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ConnectorConfiguration;
import com.latticeengines.domain.exposed.eai.DeleteFileToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordImportCounter;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.eai.runtime.service.EaiRuntimeService;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.ImportService;

@Component("deleteFileToHdfsService")
public class DeleteFileToHdfsService extends EaiRuntimeService<DeleteFileToHdfsConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(DeleteFileToHdfsService.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private EaiMetadataService eaiMetadataService;

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(DeleteFileToHdfsConfiguration config) {
        String jobDetailIds = config.getProperty(ImportProperty.EAIJOBDETAILIDS);
        List<Object> jobDetailIdsRaw = JsonUtils.deserialize(jobDetailIds, List.class);
        List<Long> eaiJobDetailIds = JsonUtils.convertList(jobDetailIdsRaw, Long.class);
        Long jobDetailId = eaiJobDetailIds.size() > 0 ? eaiJobDetailIds.get(0) : -1L;

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
        if(config.getBusinessEntity() != null && config.getBusinessEntity().equals(BusinessEntity.Transaction)) {
            context.setProperty(ImportProperty.DEDUP_ENABLE, Boolean.FALSE.toString());
        } else {
            context.setProperty(ImportProperty.DEDUP_ENABLE, Boolean.TRUE.toString());
        }
        context.setProperty(ImportProperty.SKIP_UPDATE_ATTR_NAME, Boolean.TRUE.toString());
        context.setProperty(ImportProperty.ID_COLUMN_NAME, InterfaceName.Id.name());

        Table template = eaiMetadataService.getTable(customerSpace, config.getTableName());
        log.info(String.format("Modeling metadata for template: %s", JsonUtils.serialize(template.getModelingMetadata())));
        context.setProperty(ImportProperty.METADATA, JsonUtils.serialize(template.getModelingMetadata()));

        String targetPath = createTargetPath(config.getCustomerSpace());
        List<Table> tableMetadata = new ArrayList<>();
        SourceImportConfiguration sourceImportConfig = sourceImportConfigs.get(0);
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
        for (Table table : metadata) {
            context.getProperty(ImportProperty.MULTIPLE_EXTRACT, Map.class).put(table.getName(), Boolean.FALSE);
        }
        importService.importDataAndWriteToHdfs(sourceImportConfig, context, connectorConfiguration);

        ApplicationId appId = context.getProperty(ImportProperty.APPID, ApplicationId.class);
        log.info("Application id is : " + appId.toString());
        waitForAppId(appId.toString());

        Counters counters = jobProxy.getMRJobCounters(appId.toString());
        long processedRecords = counters.getCounter(RecordImportCounter.IMPORTED_RECORDS).getValue();
        long ignoredRecords = counters.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue();
        long duplicatedRecords = counters.getCounter(RecordImportCounter.DUPLICATE_RECORDS).getValue();
        long totalRecords = processedRecords + ignoredRecords + duplicatedRecords;
        Map<String, Long> processedRecordsMap = context.getProperty(ImportProperty.PROCESSED_RECORDS, Map.class);
        processedRecordsMap.put(template.getName(), processedRecords);

        eaiMetadataService.updateTableSchema(metadata, context);
        eaiMetadataService.registerTables(metadata, context);

        setEaiJobDetailInfo(jobDetailId, (int)processedRecords, ignoredRecords, duplicatedRecords, totalRecords);
    }

    private String createTargetPath(CustomerSpace customerSpace) {
        String targetPath = String.format("%s/%s/DeleteFile/Extracts/%s",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.FILE.getName(), new SimpleDateFormat(EXTRACT_DATE_FORMAT).format(new Date()));
        return targetPath;
    }

    private void setEaiJobDetailInfo(Long jobDetailId, int processedRecords, long ignoredRecords,
                                     long duplicatedRecords, long totalRecords) {
        EaiImportJobDetail jobDetail = eaiImportJobDetailService
                .getImportJobDetailById(jobDetailId);
        if (jobDetail != null) {
            jobDetail.setProcessedRecords(processedRecords);
            jobDetail.setTotalRows(totalRecords);
            jobDetail.setIgnoredRows(ignoredRecords);
            jobDetail.setDedupedRows(duplicatedRecords);
            //when extract has processed records info means the import completed, waiting for register.
            jobDetail.setStatus(ImportStatus.SUCCESS);
            eaiImportJobDetailService.updateImportJobDetail(jobDetail);
        }
    }

}
