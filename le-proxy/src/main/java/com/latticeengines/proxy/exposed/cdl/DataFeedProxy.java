package com.latticeengines.proxy.exposed.cdl;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.AttributeLimit;
import com.latticeengines.domain.exposed.cdl.DataLimit;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.datafeed.SimpleDataFeed;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("dataFeedProxy")
public class DataFeedProxy extends MicroserviceRestApiProxy {

    private static final Logger log = LoggerFactory.getLogger(DataFeedProxy.class);

    protected DataFeedProxy() {
        super("cdl");
    }

    public DataFeedProxy(String hostPort) {
        super(hostPort, "cdl");
    }

    public DataFeed getDataFeed(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed", shortenCustomerSpace(customerSpace));
        return get("get data feed", url, DataFeed.class);
    }

    public DataFeed getDefaultDataFeed(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/default",
                shortenCustomerSpace(customerSpace));
        return get("get default data feed", url, DataFeed.class);
    }

    public List<DataFeed> getAllDataFeeds() {
        String url = constructUrl("/datafeed/internal/list");
        List<?> list = get("get all data feeds", url, List.class);
        return JsonUtils.convertList(list, DataFeed.class);
    }

    public List<SimpleDataFeed> getAllSimpleDataFeeds() {
        String url = constructUrl("/datafeed/internal/simpledatafeedlist");
        List<?> list = get("get all simple data feeds", url, List.class);
        return JsonUtils.convertList(list, SimpleDataFeed.class);
    }

    public List<SimpleDataFeed> getAllSimpleDataFeeds(TenantStatus status, String version) {
        String url = constructUrl(String.format("/datafeed/internal/simpledatafeedlist?status=%s&version=%s", status.name(), version));
        List<?> list = get("get all simple data feeds", url, List.class);
        return JsonUtils.convertList(list, SimpleDataFeed.class);
    }

    public DataFeedExecution startExecution(String customerSpace, DataFeedExecutionJobType jobType, long jobId) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/jobtype/{jobType}/startexecution",
                shortenCustomerSpace(customerSpace), jobType);
        return post("startExecution", url, jobId, DataFeedExecution.class);
    }

    public DataFeedExecution finishExecution(String customerSpace, String initialDataFeedStatus) {
        return finishExecution(customerSpace, initialDataFeedStatus, null);
    }

    public DataFeedExecution finishExecution(String customerSpace, String initialDataFeedStatus, Long executionId) {
        String base_url = "/customerspaces/{customerSpace}/datafeed/status/{initialDataFeedStatus}/finishexecution";
        String url;
        if (executionId == null) {
            url = constructUrl(base_url, shortenCustomerSpace(customerSpace), initialDataFeedStatus);
        } else {
            url = constructUrl(base_url + "?executionId={executionId}", shortenCustomerSpace(customerSpace),
                    initialDataFeedStatus, executionId);
        }
        return post("finishExecution", url, null, DataFeedExecution.class);
    }

    public DataFeedExecution failExecution(String customerSpace, String initialDataFeedStatus) {
        return failExecution(customerSpace, initialDataFeedStatus, null);
    }

    public DataFeedExecution failExecution(String customerSpace, String initialDataFeedStatus, Long executionId) {

        String base_url = "/customerspaces/{customerSpace}/datafeed/status/{initialDataFeedStatus}/failexecution";
        String url;
        if (executionId == null) {
            url = constructUrl(base_url, shortenCustomerSpace(customerSpace), initialDataFeedStatus);
        } else {
            url = constructUrl(base_url + "?executionId={executionId}", shortenCustomerSpace(customerSpace),
                    initialDataFeedStatus, executionId);
        }
        return post("failExecution", url, null, DataFeedExecution.class);
    }

    public DataFeedExecution updateExecutionWorkflowId(String customerSpace, Long workflowId) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/execution/workflow/{workflowId}",
                shortenCustomerSpace(customerSpace), workflowId);
        return post("updateExecutionWorkflowId", url, null, DataFeedExecution.class);
    }

    public void updateDataFeedDrainingStatus(String customerSpace, String drainingStatus) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/drainingstatus/{drainingStatus}",
                shortenCustomerSpace(customerSpace), drainingStatus);
        put("updateDataFeedDrainingStatus", url);
    }

    public void updateDataFeedMaintenanceMode(String customerSpace, boolean maintenanceMode) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed//maintenance/{maintenanceMode}",
                shortenCustomerSpace(customerSpace), maintenanceMode);
        put("updateDataFeedMaintenanceMode", url);
    }

    public void updateDataFeedStatus(String customerSpace, String status) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/status/{status}",
                shortenCustomerSpace(customerSpace), status);
        put("updateDataFeedStatus", url);
    }

    public void updateDataFeedNextInvokeTime(String customerSpace, Date time) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/updatenextinvoketime",
                shortenCustomerSpace(customerSpace));
        post("updateDataFeedNextInvokeTime", url, time);
    }

    public void updateDataFeedScheduleTime(String customerSpace, Boolean scheduleNow, ProcessAnalyzeRequest request) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/updatescheduletime?scheduleNow={scheduleNow}",
                shortenCustomerSpace(customerSpace), scheduleNow);
        post("updateDataFeedScheduleTime", url, request);
    }

    public Boolean dataFeedTaskExist(String customerSpace, String dataFeedType, String entity) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks/{dataFeedType}/{entity}/exist",
                shortenCustomerSpace(customerSpace), dataFeedType, entity);
        return get("dataFeedTaskExist", url, Boolean.class);
    }

    public void createDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks",
                shortenCustomerSpace(customerSpace));
        post("createDataFeedTask", url, dataFeedTask, Void.class);
    }

    public void createOrUpdateDataFeedTask(String customerSpace, String source, String feedType, String entity,
            String templateName) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeed/tasks/{source}/{dataFeedType}/{entity}/{tableName}",
                shortenCustomerSpace(customerSpace), source, feedType, entity, templateName);
        post("createOrUpdateDataFeedTask", url, null, Void.class);
    }

    public DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks/{source}/{dataFeedType}/{entity}",
                shortenCustomerSpace(customerSpace), source, dataFeedType, entity);
        return get("getDataFeedTask", url, DataFeedTask.class);
    }

    public DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks/{source}/{dataFeedType}",
                shortenCustomerSpace(customerSpace), source, dataFeedType);
        return get("getDataFeedTask", url, DataFeedTask.class);
    }

    public DataFeedTask getDataFeedTask(String customerSpace, String id) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks/{id}",
                shortenCustomerSpace(customerSpace), id);
        return get("getDataFeedTaskById", url, DataFeedTask.class);
    }

    public S3ImportSystem getImportSystemByTaskId(String customerSpace, String taskId) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks/{taskId}/getSystem",
                shortenCustomerSpace(customerSpace), taskId);
        return get("getImportSystemByTaskId", url, S3ImportSystem.class);
    }

    public Long nextInvokeTime(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/nextinvoketime",
                shortenCustomerSpace(customerSpace));
        return get("getNextInvokeTime", url, Long.class);
    }

    public List<DataFeedTask> getDataFeedTaskWithSameEntity(String customerSpace, String entity) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks/{entity}/list",
                shortenCustomerSpace(customerSpace), entity);
        List<?> res = get("getDataFeedTaskWithSameEntity", url, List.class);
        return JsonUtils.convertList(res, DataFeedTask.class);
    }

    public List<DataFeedTask> getDataFeedTaskByUniqueIds(String customerSpace, List<String> uniqueIds) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks/byuniqueids",
                shortenCustomerSpace(customerSpace));
        List<?> res = post("getDataFeedTaskWithSameEntity", url, uniqueIds, List.class);
        return JsonUtils.convertList(res, DataFeedTask.class);
    }

    public void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks",
                shortenCustomerSpace(customerSpace));
        put("updateDataFeedTask", url, dataFeedTask);
    }

    public List<String> registerExtract(String customerSpace, String taskId, String tableName, Extract extract) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks/{taskId}/registerextract/{tableName}",
                shortenCustomerSpace(customerSpace), taskId, tableName);
        List<?> res = post("registerExtract", url, extract, List.class);
        return JsonUtils.convertList(res, String.class);
    }

    public List<String> registerExtracts(String customerSpace, String taskId, String tableName,
            List<Extract> extracts) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeed/tasks/{taskId}/registerextracts/{tableName}",
                shortenCustomerSpace(customerSpace), taskId, tableName);
        List<?> res = post("registerExtract", url, extracts, List.class);
        return JsonUtils.convertList(res, String.class);
    }

    public void addTableToQueue(String customerSpace, String taskId, String tableName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks/{taskId}/addtabletoqueue/{tableName}",
                shortenCustomerSpace(customerSpace), taskId, tableName);
        put("addTableToQueue", url);
    }

    public void addTablesToQueue(String customerSpace, String taskId, List<String> tables) {
        if (tables == null || tables.size() == 0) {
            return;
        }
        String baseUrl = "/customerspaces/{customerSpace}/datafeed/tasks/{taskId}/addtabletoqueue?";
        StringBuilder builder = new StringBuilder();
        tables.forEach(tableName -> builder.append(String.format("tableName=%s&", tableName)));
        baseUrl += builder.toString();
        String url = constructUrl(baseUrl, shortenCustomerSpace(customerSpace), taskId);
        put("addTablesToQueue", url);
    }

    public List<Extract> getExtractsPendingInQueue(String customerSpace, String source, String dataFeedType,
            String entity) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeed/tasks/{source}/{dataFeedType}/{entity}/unconsolidatedextracts",
                shortenCustomerSpace(customerSpace), source, dataFeedType, entity);
        List<?> res = get("getExtractPendingInQueue", url, List.class);
        return JsonUtils.convertList(res, Extract.class);
    }

    public void resetImport(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/resetimport",
                shortenCustomerSpace(customerSpace));
        post("resetImport", url, null, Void.class);
    }

    public void resetImportByEntity(String customerSpace, String entity) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/resetimport/{entity}",
                shortenCustomerSpace(customerSpace), entity);
        post("resetImportByEntity", url, null, Void.class);
    }

    public DataFeed updateEarliestLatestTransaction(String customerSpace, Integer earliestDayPeriod,
            Integer latestDayPeriod) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeed/earliesttransaction/{earliestDayPeriod}/{latestDayPeriod}",
                shortenCustomerSpace(customerSpace), earliestDayPeriod.toString(), latestDayPeriod.toString());
        return post("updateEarliestLatestTransaction", url, null, DataFeed.class);
    }

    public DataFeed rebuildTransaction(String customerSpace, Boolean isRebuild) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/rebuildtransaction/{status}",
                shortenCustomerSpace(customerSpace), isRebuild.toString());
        return post("rebuildTransaction", url, null, DataFeed.class);
    }

    public DataLimit getDataQuotaLimitMap(CustomerSpace customerSpace) {
            log.info(customerSpace.toString());
            String tenantId = customerSpace.getTenantId();
        String url = constructUrl(String.format("/datafeed/internal/dataQuotaLimitMap?customerSpace=%s"
                , tenantId));
        return get("get all data quota limit list", url, DataLimit.class);
    }

    public List<Table> getTemplateTables(String customerSpace, String entity) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeed/tasks/{entity}/getTables",
                shortenCustomerSpace(customerSpace), entity);
        List<?> res = get("getTemplateTables", url, List.class);
        return JsonUtils.convertList(res, Table.class);
    }

    public AttributeLimit getAttributeQuotaLimit(String customerSpace) {
        String url = constructUrl(String.format("/datafeed/internal/attributeQuotaLimit?customerSpace=%s",
                shortenCustomerSpace(customerSpace)));
        return get("get all attribute quota limit", url, AttributeLimit.class);
    }
}
