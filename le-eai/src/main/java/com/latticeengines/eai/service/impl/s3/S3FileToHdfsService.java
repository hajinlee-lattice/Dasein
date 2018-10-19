package com.latticeengines.eai.service.impl.s3;

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
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.eai.ConnectorConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordImportCounter;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.eai.runtime.service.EaiRuntimeService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.proxy.exposed.cdl.CDLS3FolderProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.yarn.exposed.service.EMREnvService;

@Component("s3FileToHdfsService")
public class S3FileToHdfsService extends EaiRuntimeService<S3FileToHdfsConfiguration> {

    private static Logger log = LoggerFactory.getLogger(S3FileToHdfsService.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private CDLS3FolderProxy cdls3FolderProxy;

    @Inject
    private EMREnvService emrEnvService;

    private String hdfsFilePath;

    @Override
    public void invoke(S3FileToHdfsConfiguration config) {
        try {
            copyToHdfs(config);
            importFile(config);
        } catch (Exception e) {
            cdls3FolderProxy.moveToFailed(config.getCustomerSpace().toString(), config.getS3FilePath());
            throw e;
        }
    }

    private void copyToHdfs(S3FileToHdfsConfiguration config) {
        try {
            Path rawPath = new Path(getValiePath(config.getS3FilePath()));
            String s3nPath = rawPath.toS3NUri(config.getS3Bucket());
            Path hdfsPath = PathBuilder.buildS3FilePath(CamilleEnvironment.getPodId(), config.getCustomerSpace());
            hdfsPath = hdfsPath.append(String.valueOf(new Date().getTime()));
            hdfsPath = hdfsPath.append(config.getS3FileName());
            String queue = LedpQueueAssigner.getEaiQueueNameForSubmission();
            String overwriteQueue = LedpQueueAssigner.overwriteQueueAssignment(queue,
                    emrEnvService.getYarnQueueScheme());
            HdfsUtils.distcp(yarnConfiguration, s3nPath, hdfsPath.toString(), overwriteQueue);
            hdfsFilePath = hdfsPath.toString();
        } catch (Exception e) {
            throw new RuntimeException("Cannot copy s3 file to hdfs!" + e.getMessage());
        }
    }

    private String getValiePath(String path) {
        if (!path.startsWith("/")) {
            return "/" + path;
        } else {
            return path;
        }
    }

    @SuppressWarnings("unchecked")
    private void importFile(S3FileToHdfsConfiguration config) {
        String jobDetailIds = config.getProperty(ImportProperty.EAIJOBDETAILIDS);
        List<Object> jobDetailIdsRaw = JsonUtils.deserialize(jobDetailIds, List.class);
        List<Long> eaiJobDetailIds = JsonUtils.convertList(jobDetailIdsRaw, Long.class);
        Long jobDetailId = eaiJobDetailIds.size() > 0 ? eaiJobDetailIds.get(0) : -1L;

        try {
            List<SourceImportConfiguration> sourceImportConfigs = config.getSourceConfigurations();
            String customerSpace = config.getCustomerSpace().toString();

            ImportContext context = new ImportContext(yarnConfiguration);
            context.setProperty(ImportProperty.CUSTOMER, customerSpace);
            context.setProperty(ImportProperty.EXTRACT_PATH, new HashMap<String, String>());
            context.setProperty(ImportProperty.PROCESSED_RECORDS, new HashMap<String, Long>());
            context.setProperty(ImportProperty.LAST_MODIFIED_DATE, new HashMap<String, Long>());
            context.setProperty(ImportProperty.HDFSFILE, hdfsFilePath);
            context.setProperty(ImportProperty.MULTIPLE_EXTRACT, new HashMap<String, Boolean>());
            context.setProperty(ImportProperty.EXTRACT_PATH_LIST, new HashMap<String, List<String>>());
            context.setProperty(ImportProperty.EXTRACT_RECORDS_LIST, new HashMap<String, List<Long>>());
            if (config.getBusinessEntity() != null &&
                    (config.getBusinessEntity().equals(BusinessEntity.Transaction)
                            || config.getBusinessEntity().equals(BusinessEntity.Product))) {
                context.setProperty(ImportProperty.DEDUP_ENABLE, Boolean.FALSE.toString());
            } else {
                context.setProperty(ImportProperty.DEDUP_ENABLE, Boolean.TRUE.toString());
            }
            // CDL import won't update the attribute name to interface name.
            context.setProperty(ImportProperty.SKIP_UPDATE_ATTR_NAME, Boolean.TRUE.toString());
            context.setProperty(ImportProperty.ID_COLUMN_NAME, config.getBusinessEntity().name() + InterfaceName.Id.name());
            DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, config.getJobIdentifier());

            if (dataFeedTask == null) {
                throw new RuntimeException("Cannot find the dataFeed task for import!");
            }
            initJobDetail(jobDetailId, config.getJobIdentifier(), SourceType.FILE);
            Table template = dataFeedTask.getImportTemplate();
            log.info(String.format("Modeling metadata for template: %s",
                    JsonUtils.serialize(template.getModelingMetadata())));
            context.setProperty(ImportProperty.METADATA, JsonUtils.serialize(template.getModelingMetadata()));
            String targetPath = createTargetPath(config.getCustomerSpace(), config.getBusinessEntity(), SourceType.FILE);
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
                sourceImportConfig.getProperties().put(ImportProperty.HDFSFILE, hdfsFilePath);
                ImportService importService = ImportService.getImportService(sourceImportConfig.getSourceType());
                ConnectorConfiguration connectorConfiguration = importService.generateConnectorConfiguration("",
                        context);
                List<Table> metadata = importService.importMetadata(sourceImportConfig, context,
                        connectorConfiguration);
                tableMetadata.addAll(metadata);

                sourceImportConfig.setTables(metadata);
                for (Table table : metadata) {
                    context.getProperty(ImportProperty.MULTIPLE_EXTRACT, Map.class).put(table.getName(), Boolean.FALSE);
                }
                importService.importDataAndWriteToHdfs(sourceImportConfig, context, connectorConfiguration);

                waitAndFinalizeJob(context, template.getName(), eaiJobDetailIds.get(0));
                cdls3FolderProxy.moveToSucceed(customerSpace, config.getS3FilePath());
            }
        } catch (RuntimeException e) {
            updateJobDetailStatus(jobDetailId, ImportStatus.FAILED);
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private void waitAndFinalizeJob(ImportContext context, String templateName,
                                    Long jobDetailID) {
        ApplicationId appId = context.getProperty(ImportProperty.APPID, ApplicationId.class);

        log.info("Application id is : " + appId.toString());
        waitForAppId(appId.toString());
        Counters counters = jobProxy.getMRJobCounters(appId.toString());
        long processedRecords = counters.getCounter(RecordImportCounter.IMPORTED_RECORDS).getValue();
        log.info(String.format("Processed records: %d", processedRecords));
        Map<String, Long> processedRecordsMap = context.getProperty(ImportProperty.PROCESSED_RECORDS, Map.class);
        processedRecordsMap.put(templateName, processedRecords);

        Map<String, String> targetPathsMap = context.getProperty(ImportProperty.EXTRACT_PATH, Map.class);

        long ignoredRecords = counters.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue();
        long duplicatedRecords = counters.getCounter(RecordImportCounter.DUPLICATE_RECORDS).getValue();
        long totalRecords = processedRecords + ignoredRecords + duplicatedRecords;
        updateJobDetailExtractInfo(jobDetailID, templateName, Arrays.asList(targetPathsMap.get(templateName)),
                Arrays.asList(Long.toString(processedRecords)), totalRecords, ignoredRecords, duplicatedRecords);

    }

}
