package com.latticeengines.metadata.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.metadata.service.DataFeedTaskService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata data feed task")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/datafeed/tasks")
public class DataFeedTaskResource {

    @Autowired
    private DataFeedTaskService dataFeedTaskService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create data feed task")
    public void createDataFeedTask(@PathVariable String customerSpace, @RequestBody DataFeedTask dataFeedTask) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.createDataFeedTask(customerSpace, dataFeedTask);
    }

    @RequestMapping(value = "/{source}/{dataFeedType}/{entity}/{tableName}", method = RequestMethod.POST, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create data feed task")
    public void createOrUpdateDataFeedTask(@PathVariable String customerSpace, @PathVariable String source,
                                           @PathVariable String dataFeedType, @PathVariable String entity,
                                           @PathVariable String tableName) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.createOrUpdateDataFeedTask(customerSpace, source, dataFeedType, entity, tableName);
    }

    @RequestMapping(value = "", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update data feed task")
    public void updateDataFeedTask(@PathVariable String customerSpace, @RequestBody DataFeedTask dataFeedTask) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.updateDataFeedTask(customerSpace, dataFeedTask);
    }

    @RequestMapping(value = "/{source}/{dataFeedType}/{entity}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public DataFeedTask getDataFeedTask(@PathVariable String customerSpace, @PathVariable String source,
            @PathVariable String dataFeedType, @PathVariable String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTask(customerSpace, source, dataFeedType, entity);
    }

    @RequestMapping(value = "/{taskId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public DataFeedTask getDataFeedTask(@PathVariable String customerSpace, @PathVariable String taskId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTask(customerSpace, taskId);
    }

    @RequestMapping(value = "/{entity}/list", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public List<DataFeedTask> getDataFeedTaskWithSameEntity(@PathVariable String customerSpace, @PathVariable String entity) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return dataFeedTaskService.getDataFeedTaskWithSameEntity(customerSpace, entity);
    }

    @RequestMapping(value = "/{taskId}/registerextract/{tableName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update data feed task")
    public void registerExtract(@PathVariable String customerSpace, @PathVariable String taskId,
            @PathVariable String tableName, @RequestBody Extract extract) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.registerExtract(customerSpace, taskId, tableName, extract);
    }

    @RequestMapping(value = "/{taskId}/registerextracts/{tableName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update data feed task")
    public void registerExtracts(@PathVariable String customerSpace, @PathVariable String taskId,
            @PathVariable String tableName, @RequestBody List<Extract> extracts) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        dataFeedTaskService.registerExtracts(customerSpace, taskId, tableName, extracts);
    }

    @RequestMapping(value = "/{source}/{dataFeedType}/{entity}/unconsolidatedextracts", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get unconsolidated extracts in queue")
    public List<Extract> getExtractsPendingInQueue(@PathVariable String customerSpace, @PathVariable String source,
            @PathVariable String dataFeedType, @PathVariable String entity) {
        return dataFeedTaskService.getExtractsPendingInQueue(customerSpace, source, dataFeedType, entity);
    }

}
