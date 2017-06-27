package com.latticeengines.proxy.exposed.metadata;

import static com.latticeengines.proxy.exposed.ProxyUtils.shortenCustomerSpace;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.DataFeed;
import com.latticeengines.domain.exposed.metadata.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Extract;
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

    public void updateDataFeedStatus(String customerSpace, String status) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/status/{status}",
                shortenCustomerSpace(customerSpace), status);
        put("updateDataFeedStatus", url, null);
    }

    public DataFeedExecution retryLatestExecution(String customerSpace, String datafeedName) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeed/restartexecution",
                customerSpace, datafeedName);
        return post("restartExecution", url, null, DataFeedExecution.class);
    }

    public Boolean dataFeedTaskExist(String customerSpace, String dataFeedType, String entity) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeedtask/exist/{dataFeedType}/{entity}",
                customerSpace, dataFeedType, entity);
        return get("dataFeedTaskExist", url, Boolean.class);
    }

    public void createDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        String dataFeedName = getDataFeed(customerSpace).getName();
        String url = constructUrl("/customerspaces/{customerSpace}/datafeedtask/{dataFeedName}/create", customerSpace,
                dataFeedName);
        post("createDataFeedTask", url, dataFeedTask, Void.class);
    }

    public DataFeedTask getDataFeedTask(String customerSpace, String source, String dataFeedType, String entity) {
        String dataFeedName = getDataFeed(customerSpace).getName();
        String url = constructUrl(
                "/customerspaces/{customerSpace}/datafeedtask/{source}/{dataFeedType}/{entity}/{dataFeedName}",
                customerSpace, source, dataFeedType, entity, dataFeedName);
        return get("getDataFeedTask", url, DataFeedTask.class);
    }

    public DataFeedTask getDataFeedTask(String customerSpace, String id) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeedtask/{id}", customerSpace, id);
        return get("getDataFeedTaskById", url, DataFeedTask.class);
    }

    public void updateDataFeedTask(String customerSpace, DataFeedTask dataFeedTask) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeedtask/update", customerSpace);
        post("updateDataFeedTask", url, dataFeedTask, Void.class);
    }

    public void registerExtract(String customerSpace, String taskId, String tableName, Extract extract) {
        String url = constructUrl("/customerspaces/{customerSpace}/datafeedtask/registerextract/{taskId}/{tableName}",
                customerSpace, taskId, tableName);
        post("registerExtract", url, extract, Void.class);
    }

}
