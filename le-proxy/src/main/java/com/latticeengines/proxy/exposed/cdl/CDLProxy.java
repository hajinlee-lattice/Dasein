package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.CleanupAllConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupByDateRangeConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.MaintenanceOperationType;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.eai.S3FileToHdfsConfiguration;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
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
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/processanalyze",
                shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("process and analyze", url, request, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
        } else {
            throw new RuntimeException(
                    "Failed to start processAnalyze job: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public ApplicationId restartProcessAnalyze(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/processanalyze/restart",
                shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("restart process and analyze", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
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
        return createDataFeedTask(customerSpace, source, entity, feedType, subType, displayName, false, metadata);
    }

    @SuppressWarnings("unchecked")
    public String createDataFeedTask(String customerSpace, String source, String entity, String feedType,
                                     String subType, String displayName, boolean sendEmail, CDLImportConfig metadata) {
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

    @SuppressWarnings("unchecked")
    public ApplicationId submitImportJob(String customerSpace, String taskIdentifier, CDLImportConfig importConfig) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/datafeed/tasks/import/internal" + "/{taskIdentifier}",
                customerSpace, taskIdentifier);
        ResponseDocument<String> responseDoc = post("submitImportJob", url, importConfig, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
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
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
        } else {
            throw new RuntimeException(
                    "Failed to submit s3 import job: " + StringUtils.join(responseDoc.getErrors(), ","));
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

    public ApplicationId OrphanRecordsExport(String customerSpace, OrphanRecordsExportRequest request) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/datafeed/exportorphanrecords", customerSpace);
        ResponseDocument responseDoc = post("orphanRecordExport", url, request, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            String appIdStr = responseDoc.getResult().toString();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
        } else {
            throw new RuntimeException(
                    "Failed to start OrphanRecordsExport job: " + StringUtils.join(responseDoc.getErrors(), ","));
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
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
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
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
        } else {
            throw new RuntimeException("Failed to cleanupAllData: " + StringUtils.join(responseDoc.getErrors(), ","));
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
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
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
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
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
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
        } else {
            throw new RuntimeException("Failed to cleanupByUpload: " + StringUtils.join(responseDoc.getErrors(), ","));
        }
    }
}
