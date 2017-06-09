package com.latticeengines.metadata.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.metadata.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.metadata.service.DataFeedTaskService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metadata", description = "REST resource for metadata data feed task")
@RestController
@RequestMapping(value = "/customerspaces/{customerSpace}/datafeedtask")
public class DataFeedTaskResource {

    @Autowired
    private DataFeedTaskService dataFeedTaskService;

    @RequestMapping(value = "{dataFeedName}/create", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create data feed task")
    public void createDataFeedTask(@PathVariable String customerSpace, @PathVariable String dataFeedName,
                                   @RequestBody DataFeedTask dataFeedTask, HttpServletRequest request) {
        dataFeedTaskService.createDataFeedTask(customerSpace, dataFeedName, dataFeedTask);
    }

    @RequestMapping(value = "/update", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update data feed task")
    public void updateDataFeedTask(@PathVariable String customerSpace, @RequestBody DataFeedTask dataFeedTask,
                                   HttpServletRequest request) {
        dataFeedTaskService.updateDataFeedTask(customerSpace, dataFeedTask);
    }

    @RequestMapping(value = "/{source}/{dataFeedType}/{entity}/{dataFeedName}",
            method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public DataFeedTask getDataFeedTask(@PathVariable String customerSpace, @PathVariable String source,
                                        @PathVariable String dataFeedType, @PathVariable String entity,
                                        @PathVariable String dataFeedName,
                                        HttpServletRequest request) {
        return dataFeedTaskService.getDataFeedTask(customerSpace, source, dataFeedType, entity, dataFeedName);
    }

    @RequestMapping(value = "/{taskId}", method = RequestMethod.GET, headers =
            "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get data feed task")
    public DataFeedTask getDataFeedTask(@PathVariable String customerSpace, @PathVariable Long taskId,
                                        HttpServletRequest request) {
        return dataFeedTaskService.getDataFeedTask(taskId);
    }

    @RequestMapping(value = "/registerextract/{taskId}", method = RequestMethod.POST,
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update data feed task")
    public void registerExtract(@PathVariable String customerSpace, @PathVariable Long taskId,
                                @RequestBody Extract extract, HttpServletRequest request) {
        dataFeedTaskService.registerExtract(customerSpace, taskId, extract);
    }

}
