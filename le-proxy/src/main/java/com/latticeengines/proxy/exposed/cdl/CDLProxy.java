package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.cdl.CleanupByUploadConfiguration;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.cdl.CDLImportConfig;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("cdlProxy")
public class CDLProxy extends MicroserviceRestApiProxy {

    protected CDLProxy() {
        super("cdl");
    }

    @SuppressWarnings("unchecked")
    public ApplicationId consolidate(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/consolidate", shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("consolidate", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        String appIdStr = responseDoc.getResult();
        return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
    }

    @SuppressWarnings("unchecked")
    public ApplicationId processAnalyze(String customerSpace, ProcessAnalyzeRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/processanalyze", shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("process and analyze", url, request, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        String appIdStr = responseDoc.getResult();
        return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
    }

    @SuppressWarnings("unchecked")
    public ApplicationId profile(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/profile", shortenCustomerSpace(customerSpace));
        ResponseDocument<String> responseDoc = post("profile", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        String appIdStr = responseDoc.getResult();
        return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
    }

    @SuppressWarnings("unchecked")
    public boolean reset(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/reset", shortenCustomerSpace(customerSpace));
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
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/tasks/create" +
                        "?source={source}&feedtype={feedtype}&entity={entity}",
                shortenCustomerSpace(customerSpace), source, feedType, entity);
        ResponseDocument<String> responseDoc = post("createDataFeedTask", url, metadata, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            return responseDoc.getResult();
        } else {
            throw new RuntimeException("Failed to create data feed task: " +
                    StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public ApplicationId submitImportJob(String customerSpace, String taskIdentifier, CDLImportConfig importConfig) {
        String url = constructUrl("/customerspaces/{customerSpace}/datacollection/datafeed/tasks/import/internal" +
                "/{taskIdentifier}", customerSpace, taskIdentifier);
        ResponseDocument<String> responseDoc = post("submitImportJob", url, importConfig, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
        } else {
            throw new RuntimeException("Failed to submit import job: " +
                    StringUtils.join(responseDoc.getErrors(), ","));
        }
    }


    @SuppressWarnings("unchecked")
    public ApplicationId consolidateManually(String customerSpace) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datacollection/datafeed/consolidate?" + "draining={draining}",
                shortenCustomerSpace(customerSpace), "true");
        ResponseDocument<String> responseDoc = post("consolidate", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
        } else {
            throw new RuntimeException("Failed to submit import job: " +
                    StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public ApplicationId cleanupAll(String customerSpace, BusinessEntity entity) {
        List<Object> args = new ArrayList<>();
        args.add(customerSpace);
        String urlPattern = "/customerspaces/{customerSpace}/datacleanup/all";

        if (entity != null) {
            args.add(entity);
            urlPattern += "?BusinessEntity={BusinessEntity}";
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        ResponseDocument<String> responseDoc = post("cleanup all", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
        } else {
            throw new RuntimeException("Failed to submit import job: " +
                    StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public ApplicationId cleanupAllData(String customerSpace, BusinessEntity entity) {
        List<Object> args = new ArrayList<>();
        args.add(customerSpace);
        String urlPattern = "/customerspaces/{customerSpace}/datacleanup/alldata";

        if (entity != null) {
            args.add(entity);
            urlPattern += "?BusinessEntity={BusinessEntity}";
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        ResponseDocument<String> responseDoc = post("cleanup all data", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
        } else {
            throw new RuntimeException("Failed to submit import job: " +
                    StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public ApplicationId cleanupByTimeRange(String customerSpace, String startTime, String endTime, BusinessEntity
            entity) {
        List<Object> args = new ArrayList<>();
        args.add(customerSpace);
        args.add(startTime);
        args.add(endTime);
        String urlPattern = "/customerspaces/{customerSpace}/datacleanup/bytimerange?startTime={startTime}&endTime={endTime}";

        if (entity != null) {
            args.add(entity);
            urlPattern += "&BusinessEntity={BusinessEntity}";
        }
        String url = constructUrl(urlPattern, args.toArray(new Object[args.size()]));
        ResponseDocument<String> responseDoc = post("cleanup by time range", url, null, ResponseDocument.class);
        if (responseDoc == null) {
            return null;
        }
        if (responseDoc.isSuccess()) {
            String appIdStr = responseDoc.getResult();
            return StringUtils.isBlank(appIdStr) ? null : ConverterUtils.toApplicationId(appIdStr);
        } else {
            throw new RuntimeException("Failed to submit import job: " +
                    StringUtils.join(responseDoc.getErrors(), ","));
        }
    }

    @SuppressWarnings("unchecked")
    public ResponseDocument<String> cleanupByUpload(String customerSpace, String tableName, String filePath, BusinessEntity
            entity, CleanupOperationType operationType) {
        CleanupByUploadConfiguration configuration = new CleanupByUploadConfiguration();
        configuration.setTableName(tableName);
        configuration.setFilePath(filePath);
        configuration.setEntity(entity);
        configuration.setCleanupOperationType(operationType);

        String url = constructUrl("/customerspaces/{customerSpace}/datacleanup/byupload", customerSpace);

        return post("cleanup by upload", url, configuration, ResponseDocument.class);
    }
}
