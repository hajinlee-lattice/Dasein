package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("cdlProxy")
public class CDLProxy extends MicroserviceRestApiProxy {

    protected CDLProxy() {
        super("cdl");
    }

    @SuppressWarnings("unchecked")
    public ApplicationId processAnalyze(String customerSpace, ProcessAnalyzeRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/processanalyze",
                shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("process and analyze", url, request, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        String appIdStr = responseDoc.getResult();
        return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
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

    @SuppressWarnings("unchecked")
    public String createDataFeedTask(String customerSpace, String source, String entity, String feedType,
            CDLImportConfig metadata) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/datafeed/tasks/create"
                        + "?source={source}&feedtype={feedtype}&entity={entity}",
                shortenCustomerSpace(customerSpace), source, feedType, entity);
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
    public ResponseDocument<String> cleanupByUpload(String customerSpace, String tableName, String filePath,
            BusinessEntity entity, CleanupOperationType operationType, String initiator) {
        CleanupByUploadConfiguration configuration = new CleanupByUploadConfiguration();
        configuration.setTableName(tableName);
        configuration.setFilePath(filePath);
        configuration.setEntity(entity);
        configuration.setCleanupOperationType(operationType);
        configuration.setOperationInitiator(initiator);

        String url = constructUrl("/customerspaces/{customerSpace}/datacleanup", customerSpace);

        return post("cleanup by upload", url, configuration, ResponseDocument.class);
    }
}
