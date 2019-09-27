package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.BulkEntityMatchRequest;
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.CleanupAllConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupByDateRangeConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreToImportRequest;
import com.latticeengines.domain.exposed.cdl.EntityExportRequest;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationType;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.SimpleTemplateMetadata;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingStatus;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;
import com.latticeengines.proxy.exposed.ProxyInterface;

@Component("cdlProxy")
public class CDLProxy extends MicroserviceRestApiProxy implements ProxyInterface {

    protected CDLProxy() {
        super("cdl");
    }

    public CDLProxy(String hostPort) {
        super(hostPort, "cdl");
    }

    @SuppressWarnings("unchecked")
    public ApplicationId processAnalyze(String customerSpace, ProcessAnalyzeRequest request) {
        return scheduleProcessAnalyze(customerSpace, true, request);
    }

    @SuppressWarnings("unchecked")
    public ApplicationId scheduleProcessAnalyze(String customerSpace, boolean runNow, ProcessAnalyzeRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/processanalyze?runNow={runNow}",
                shortenCustomerSpace(customerSpace), runNow);
        ResponseDocument<String> responseDoc = post("process and analyze", url, request, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
        } else {
            throw new RuntimeException(
                    "Failed to start processAnalyze job: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    public ApplicationId restartProcessAnalyze(String customerSpace) {
        return restartProcessAnalyze(customerSpace, null);
    }

    @SuppressWarnings("unchecked")
    public ApplicationId restartProcessAnalyze(String customerSpace, Boolean autoRetry) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/processanalyze/restart",
                shortenCustomerSpace(customerSpace));
        if (autoRetry != null) {
            url += "?autoRetry=" + autoRetry;
        }
        ResponseDocument<String> responseDoc = post("restart process and analyze", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
        } else {
            throw new RuntimeException(
                    "Failed to start processAnalyze job: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public boolean reset(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/reset",
                shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("kickoff reset", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return false;
        }
        String statusStr = responseDoc.getResult();
        return ("Success".equals(statusStr));
    }

    public String createDataFeedTask(String customerSpace, String source, String entity, String feedType,
                                     String subType, String displayName, CDLImportConfig metadata) {
        return createDataFeedTask(customerSpace, source, entity, feedType, subType, displayName, false, "", metadata);
    }

    @SuppressWarnings("unchecked")
    public String createDataFeedTask(String customerSpace, String source, String entity, String feedType,
                                     String subType, String displayName, boolean sendEmail, String user,
                                     CDLImportConfig metadata) {
        String baseUrl = "/customerspaces/{customerSpace}/datacollection/datafeed/tasks/create"
                + "?source={source}&feedtype={feedtype}&entity={entity}&sendEmail={sendEmail}";
        List<String> args = new ArrayList<>();
        args.add(shortenCustomerSpace(customerSpace));
        args.add(source);
        args.add(feedType);
        args.add(entity);
        args.add(String.valueOf(sendEmail));
        if (StringUtils.isNotBlank(subType)) {
            baseUrl += "&subType={subType}";
            args.add(subType);
        }
        if (StringUtils.isNotBlank(displayName)) {
            baseUrl += "&displayName={displayName}";
            args.add(displayName);
        }
        if (StringUtils.isNotBlank(user)) {
            baseUrl += "&user={user}";
            args.add(user);
        }

        String url = constructUrl(baseUrl, args.toArray());
        ResponseDocument<String> responseDoc = post("createDataFeedTask", url, metadata, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return responseDoc.getResult();
        } else {
            throw new RuntimeException(
                    "Failed to create data feed task: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    public ApplicationId submitImportJob(String customerSpace, String taskIdentifier, CDLImportConfig importConfig) {
        return submitImportJob(customerSpace, taskIdentifier, false, importConfig);
    }

    @SuppressWarnings("unchecked")
    public ApplicationId submitImportJob(String customerSpace, String taskIdentifier, boolean onlyData,
                                         CDLImportConfig importConfig) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/datafeed/tasks/import/internal/{taskIdentifier}?onlyData={onlyData}",
                customerSpace, taskIdentifier, String.valueOf(onlyData));
        ResponseDocument<String> responseDoc = post("submitImportJob", url, importConfig, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
        } else {
            throw new RuntimeException(
                    "Failed to submit import job: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public ApplicationId submitS3ImportJob(String customerSpace, S3FileToHdfsConfiguration s3FileToHdfsConfiguration) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/datafeed/tasks/s3import", customerSpace);
        ResponseDocument<String> responseDoc = post("submitS3ImportJob", url, s3FileToHdfsConfiguration, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
        } else {
            throw new LedpException(LedpCode.LEDP_40056,
                    new String[] { StringUtils.join(responseDoc.getErrors(), ",") });
        }
    }

    @SuppressWarnings("unchecked")
    public boolean resetImport(String customerSpace, BusinessEntity entity) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/datafeed/tasks/reset", customerSpace);
        if (entity != null) {
            url += "?entity=" + entity.name();
        }
        ResponseDocument<Boolean> responseDoc = post("resetImport", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return false;
        }
        return responseDoc.isSuccess();
    }

    @SuppressWarnings("unchecked")
    public ApplicationId submitBulkEntityMatch(String customerSpace, BulkEntityMatchRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/match/entity/bulk", customerSpace);
        ResponseDocument<String> res = post("bulkEntityMatch", url, request, ResponseDocument.class);
        if (res == null) {
            return null;
        }
        if (res.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(res.getResult());
        } else {
            throw new RuntimeException("Fail to submit bulk entity match job, errors = " + res.getErrors());
        }
    }

    public ApplicationId submitOrphanRecordsExport(String customerSpace, OrphanRecordsExportRequest request) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/datafeed/exportorphanrecords", customerSpace);
        ResponseDocument responseDoc = post("orphanRecordExport", url, request, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult().toString());
        } else {
            throw new RuntimeException(
                    "Failed to submit orphanRecordsExport job: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }


    @SuppressWarnings("unchecked")
    public ApplicationId cleanupAll(String customerSpace, BusinessEntity entity, String initiator) {
        String urlPattern = "/customerspaces/{customerSpace}/datacleanup";
        String url = constructUrl(urlPattern, customerSpace);
        CleanupAllConfiguration cleanupAllConfiguration = new CleanupAllConfiguration();
        cleanupAllConfiguration.setOperationType(MaintenanceOperationType.DELETE);
        cleanupAllConfiguration.setCleanupOperationType(CleanupOperationType.ALL);
        cleanupAllConfiguration.setEntity(entity);
        cleanupAllConfiguration.setCustomerSpace(customerSpace);
        cleanupAllConfiguration.setOperationInitiator(initiator);
        ResponseDocument<String> responseDoc = post("cleanup all", url, cleanupAllConfiguration,
                ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
        } else {
            throw new RuntimeException("Failed to cleanupAll: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public ApplicationId cleanupAllData(String customerSpace, BusinessEntity entity, String initiator) {
        String urlPattern = "/customerspaces/{customerSpace}/datacleanup";
        String url = constructUrl(urlPattern, customerSpace);
        CleanupAllConfiguration cleanupAllConfiguration = new CleanupAllConfiguration();
        cleanupAllConfiguration.setOperationType(MaintenanceOperationType.DELETE);
        cleanupAllConfiguration.setCleanupOperationType(CleanupOperationType.ALLDATA);
        cleanupAllConfiguration.setEntity(entity);
        cleanupAllConfiguration.setCustomerSpace(customerSpace);
        cleanupAllConfiguration.setOperationInitiator(initiator);
        ResponseDocument<String> responseDoc = post("cleanup all data", url, cleanupAllConfiguration,
                ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
        } else {
            throw new RuntimeException("Failed to cleanupAllData: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public void cleanupAllByAction(String customerSpace, BusinessEntity entity, String initiator) {
        String urlPattern = "/customerspaces/{customerSpace}/datacleanup/replaceAction";
        String url = constructUrl(urlPattern, customerSpace);
        CleanupAllConfiguration cleanupAllConfiguration = new CleanupAllConfiguration();
        cleanupAllConfiguration.setOperationType(MaintenanceOperationType.DELETE);
        cleanupAllConfiguration.setCleanupOperationType(CleanupOperationType.ALLDATA);
        cleanupAllConfiguration.setEntity(entity);
        cleanupAllConfiguration.setCustomerSpace(customerSpace);
        cleanupAllConfiguration.setOperationInitiator(initiator);
        post("create replace data action", url, cleanupAllConfiguration);
    }

    @SuppressWarnings("unchecked")
    public ApplicationId cleanupAllAttrConfig(String customerSpace, BusinessEntity entity, String initiator) {
        String urlPattern = "/customerspaces/{customerSpace}/datacleanup";
        String url = constructUrl(urlPattern, customerSpace);
        CleanupAllConfiguration cleanupAllConfiguration = new CleanupAllConfiguration();
        cleanupAllConfiguration.setOperationType(MaintenanceOperationType.DELETE);
        cleanupAllConfiguration.setCleanupOperationType(CleanupOperationType.ALLATTRCONFIG);
        cleanupAllConfiguration.setEntity(entity);
        cleanupAllConfiguration.setCustomerSpace(customerSpace);
        cleanupAllConfiguration.setOperationInitiator(initiator);
        ResponseDocument<String> responseDoc = post("cleanup all attr Config", url, cleanupAllConfiguration,
                ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
        } else {
            throw new RuntimeException(
                    "Failed to cleanupAllAttrConfig: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public ApplicationId convertBatchStoreToImport(String customerSpace, ConvertBatchStoreToImportRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/convertbatchstoretoimport",
                shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("convert batchstore to import", url, request,
                ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
        } else {
            throw new RuntimeException(
                    "Failed to start convert batchstore job: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public ApplicationId migrateImport(String customerSpace, String userId) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/migrateimport",
                shortenCustomerSpace(customerSpace));
        if (StringUtils.isEmpty(userId)) {
            userId = "DEFAULT_MIGRATE_IMPORT_USER";
        }
        ResponseDocument<String> responseDoc = post("Migrate current import to entity match style", url, userId,
                ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
        } else {
            throw new RuntimeException(
                    "Failed to start migrate import job: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public ApplicationId cleanupByTimeRange(String customerSpace, String startTime, String endTime,
            BusinessEntity entity, String initiator) throws ParseException {
        String urlPattern = "/customerspaces/{customerSpace}/datacleanup";
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date start = dateFormat.parse(startTime);
        Date end = dateFormat.parse(endTime);
        String url = constructUrl(urlPattern, customerSpace);
        CleanupByDateRangeConfiguration cleanupByDateRangeConfiguration = new CleanupByDateRangeConfiguration();
        cleanupByDateRangeConfiguration.setOperationType(MaintenanceOperationType.DELETE);
        cleanupByDateRangeConfiguration.setCleanupOperationType(CleanupOperationType.BYDATERANGE);
        cleanupByDateRangeConfiguration.setStartTime(start);
        cleanupByDateRangeConfiguration.setEndTime(end);
        cleanupByDateRangeConfiguration.setEntity(entity);
        cleanupByDateRangeConfiguration.setCustomerSpace(customerSpace);
        cleanupByDateRangeConfiguration.setOperationInitiator(initiator);
        ResponseDocument<String> responseDoc = post("cleanup by time range", url, cleanupByDateRangeConfiguration,
                ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
        } else {
            throw new RuntimeException(
                    "Failed to cleanupByTimeRange: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public ApplicationId cleanupByUpload(String customerSpace, SourceFile sourceFile, BusinessEntity entity,
            CleanupOperationType operationType, String initiator) {
        CleanupByUploadConfiguration configuration = new CleanupByUploadConfiguration();
        configuration.setTableName(sourceFile.getTableName());
        configuration.setFilePath(sourceFile.getPath());
        configuration.setFileName(sourceFile.getName());
        configuration.setFileDisplayName(sourceFile.getDisplayName());
        configuration.setEntity(entity);
        configuration.setCleanupOperationType(operationType);
        configuration.setOperationInitiator(initiator);

        String url = constructUrl("/customerspaces/{customerSpace}/datacleanup", customerSpace);

        ResponseDocument<String> responseDoc = post("cleanup by upload", url, configuration, ResponseDocument.class);

        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
        } else {
            throw new RuntimeException("Failed to cleanupByUpload: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public ApplicationId cleanupByUpload(String customerSpace, String tableName, BusinessEntity entity,
            CleanupOperationType operationType, String initiator) {
        CleanupByUploadConfiguration configuration = new CleanupByUploadConfiguration();
        configuration.setTableName(tableName);
        configuration.setUseDLData(true);
        configuration.setFilePath("");
        configuration.setFileName("VisiDB_Import");
        configuration.setFileDisplayName("VisiDB_Import");
        configuration.setEntity(entity);
        configuration.setCleanupOperationType(operationType);
        configuration.setOperationInitiator(initiator);

        String url = constructUrl("/customerspaces/{customerSpace}/datacleanup", customerSpace);

        ResponseDocument<String> responseDoc = post("cleanup by upload", url, configuration, ResponseDocument.class);

        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
        } else {
            throw new RuntimeException("Failed to cleanupByUpload: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public void createS3ImportSystem(String customerSpace, S3ImportSystem system) {
        String url = constructUrl("/customerspaces/{customerSpace}/s3import/system", shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("create s3 import system", url, system, ResponseDocument.class);
        if (responseDoc == null) {
            throw new RuntimeException("Failed to create Import System!");
        }
        if (!responseDoc.isSuccess()) {
            throw new LedpException(LedpCode.LEDP_18216, responseDoc.getErrors().toArray());
        }
    }

    public S3ImportSystem getS3ImportSystem(String customerSpace, String systemName) {
        String url;
        try {
            url = constructUrl("/customerspaces/{customerSpace}/s3import/system?systemName={systemName}",
                    shortenCustomerSpace(customerSpace), URLEncoder.encode(systemName, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Cannot encode systemName: " + systemName);
        }
        return get("get s3 import system", url, S3ImportSystem.class);
    }

    public List<S3ImportSystem> getS3ImportSystemList(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/s3import/system/list",
                    shortenCustomerSpace(customerSpace));
        List<?> rawlist = get("get s3 import system list", url, List.class);
        return JsonUtils.convertList(rawlist, S3ImportSystem.class);
    }

    @SuppressWarnings("unchecked")
    public void updateS3ImportSystem(String customerSpace, S3ImportSystem importSystem) {
        String url = constructUrl("/customerspaces/{customerSpace}/s3import/system/update",
                shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("update s3 import system", url, importSystem,
                ResponseDocument.class);
        if (responseDoc == null) {
            throw new RuntimeException("Cannot update S3 Import System!");
        }
        if (!responseDoc.isSuccess()) {
            throw new LedpException(LedpCode.LEDP_40061, responseDoc.getErrors().toArray());
        }

    }

    @SuppressWarnings("unchecked")
    public void updateAllS3ImportSystemPriority(String customerSpace, List<S3ImportSystem> systemList) {
        String url = constructUrl("/customerspaces/{customerSpace}/s3import/system/list",
                shortenCustomerSpace(customerSpace));
        ResponseDocument<Boolean> responseDoc = post("update all import system priority", url, systemList,
                ResponseDocument.class);
        if (responseDoc == null) {
            throw new RuntimeException("Cannot update all import system priority!");
        }
        if (!responseDoc.isSuccess()) {
            throw new LedpException(LedpCode.LEDP_40064, responseDoc.getErrors().toArray());
        }
    }

    @SuppressWarnings("unchecked")
    public ApplicationId entityExport(String customerSpace, EntityExportRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/entityexport",
                shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("entity export", url, request, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return ApplicationIdUtils.toApplicationIdObj(responseDoc.getResult());
        } else {
            throw new RuntimeException(
                    "Failed to start entityExport job: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public boolean createWebVisitTemplate(String customerSpace, SimpleTemplateMetadata simpleTemplateMetadata) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/tasks/setup/webvisit",
                shortenCustomerSpace(customerSpace));
        ResponseDocument<Boolean> responseDoc = post("create webvisit template", url, simpleTemplateMetadata,
                ResponseDocument.class);
        if (responseDoc.isSuccess()) {
            return responseDoc.getResult();
        } else {
            return false;
        }
    }

    public Boolean isActivityBasedPA(String schedulerName) {
        String url = constructUrl("/schedulingPAQueue/isActivityBasedPA/{schedulerName}", schedulerName);
        return get("get isActivityBasedPA Flag", url, Boolean.class);
    }

    /*
     * check if scheduler is enabled for current stack
     */
    public boolean isActivityBasedPA() {
        String url = constructUrl("/schedulingPAQueue/isActivityBasedPA");
        return get("get isActivityBasedPA Flag for current stack", url, Boolean.class);
    }

    public SchedulingStatus getSchedulingStatus(String customerSpace) {
        String url = constructUrl("/schedulingPAQueue/status/{customerSpace}", shortenCustomerSpace(customerSpace));
        return get("get schedulingStatus for tenant", url, SchedulingStatus.class);
    }
}
