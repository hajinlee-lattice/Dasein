package com.latticeengines.proxy.exposed.metadata;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedProfile;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("dataFeedProxy")
public class DataFeedProxy extends MicroserviceRestApiProxy {

    protected DataFeedProxy() {
        super("metadata");
    }

    public DataFeed getDataFeed(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed", shortenCustomerSpace(customerSpace));
        return get("get data feed", url, DataFeed.class);
    }

    public DataFeed getDefaultDataFeed(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/default", shortenCustomerSpace(customerSpace));
        return get("get default data feed", url, DataFeed.class);
    }

    public List<DataFeed> getAllDataFeeds() {
        String url = constructUrl("/datafeed/internal/list");
        List<?> list = get("get all data feeds", url, List.class);
        return JsonUtils.convertList(list, DataFeed.class);
    }

    public DataFeedExecution startExecution(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/startexecution",
                shortenCustomerSpace(customerSpace));
        return post("startExecution", url, null, DataFeedExecution.class);
    }

    public DataFeedExecution finishExecution(String customerSpace, String initialDataFeedStatus) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeed/status/{initialDataFeedStatus}/finishexecution",
                shortenCustomerSpace(customerSpace), initialDataFeedStatus);
        return post("finishExecution", url, null, DataFeedExecution.class);
    }

    public DataFeedExecution failExecution(String customerSpace, String initialDataFeedStatus) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeed/status/{initialDataFeedStatus}/failexecution",
                shortenCustomerSpace(customerSpace), initialDataFeedStatus);
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
        put("updateDataFeedDrainingStatus", url, null);
    }

    public void updateDataFeedStatus(String customerSpace, String status) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/status/{status}",
                shortenCustomerSpace(customerSpace), status);
        put("updateDataFeedStatus", url, null);
    }

    public DataFeedExecution retryLatestExecution(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/restartexecution",
                shortenCustomerSpace(customerSpace));
        return post("restartExecution", url, null, DataFeedExecution.class);
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

    public DataFeedTask getDataFeedTask(String customerSpace, String id) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks/{id}",
                shortenCustomerSpace(customerSpace), id);
        return get("getDataFeedTaskById", url, DataFeedTask.class);
    }

    @SuppressWarnings("unchecked")
    public List<DataFeedTask> getDataFeedTaskWithSameEntity(String customerSpace, String entity) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks/{entity}/list",
                shortenCustomerSpace(customerSpace), entity);
        List<?> res = get("getDataFeedTaskWithSameEntity", url, List.class);
        return JsonUtils.convertList(res, DataFeedTask.class);
    }

    public void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks",
                shortenCustomerSpace(customerSpace));
        put("updateDataFeedTask", url, dataFeedTask);
    }

    public void registerExtract(String customerSpace, String taskId, String tableName, Extract extract) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/tasks/{taskId}/registerextract/{tableName}",
                shortenCustomerSpace(customerSpace), taskId, tableName);
        post("registerExtract", url, extract, Void.class);
    }

    public void registerExtracts(String customerSpace, String taskId, String tableName, List<Extract> extracts) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeed/tasks/{taskId}/registerextracts/{tableName}",
                shortenCustomerSpace(customerSpace), taskId, tableName);
        post("registerExtract", url, extracts, Void.class);
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

    public DataFeedProfile startProfile(String customerSpace) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/startprofile",
                shortenCustomerSpace(customerSpace));
        return post("startProfile", url, null, DataFeedProfile.class);
    }

    public DataFeedExecution finishProfile(String customerSpace, String initialDataFeedStatus) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeed/status/{initialDataFeedStatus}/finishprofile",
                shortenCustomerSpace(customerSpace), initialDataFeedStatus);
        return post("finishProfile", url, null, DataFeedExecution.class);
    }

    public DataFeed updateEarliestTransaction(String customerSpace, Integer transactionDayPeriod) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeed/earliesttransaction/{transactionDayPeriod}",
                shortenCustomerSpace(customerSpace), transactionDayPeriod.toString());
        return post("updateEarliestTransaction", url, null, DataFeed.class);
    }

    public DataFeed rebuildTransaction(String customerSpace, Boolean isRebuild) {
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeed/rebuildtransaction/{status}",
                shortenCustomerSpace(customerSpace), isRebuild.toString());
        return post("updateEarliestTransaction", url, null, DataFeed.class);
    }

    public DataFeedProfile updateProfileWorkflowId(String customerSpace, Long workflowId) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/profile/workflow/{workflowId}",
                shortenCustomerSpace(customerSpace), workflowId);
        return post("updateProfileWorkflowId", url, null, DataFeedProfile.class);
    }

}
