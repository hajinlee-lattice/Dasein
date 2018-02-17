package com.latticeengines.metadata.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.metadata.service.DataFeedService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafeeds", description = "REST resource for metadata data feeds")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datafeeds")
public class DataFeedResource {

    @Autowired
    private DataFeedService datafeedService;

    @RequestMapping(value = "/{datafeedName}/startexecution", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "start data feed execution")
    public DataFeedExecution startExecution(@PathVariable String customerSpace, //
            @PathVariable String datafeedName) {
        return datafeedService.startExecution(customerSpace, datafeedName);
    }

    @RequestMapping(value = "/{datafeedName}/jobtype/{jobType}/lockexecution", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "lock data feed execution")
    public ResponseDocument<Boolean> lockExecution(@PathVariable String customerSpace, //
            @PathVariable String datafeedName, //
            @PathVariable DataFeedExecutionJobType jobType) {
        return ResponseDocument.successResponse(datafeedService.lockExecution(customerSpace, datafeedName, jobType));
    }

    @RequestMapping(value = "/{datafeedName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "find data feed by name")
    public DataFeed findDataFeedByName(@PathVariable String customerSpace, @PathVariable String datafeedName) {
        return datafeedService.findDataFeedByName(customerSpace, datafeedName);
    }

    @RequestMapping(value = "/{datafeedName}/status/{status}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "update data feed status by name")
    public void updateDataFeedStatus(@PathVariable String customerSpace, @PathVariable String datafeedName,
            @PathVariable String status) {
        datafeedService.updateDataFeed(customerSpace, datafeedName, status);
    }

    @RequestMapping(value = "/{datafeedName}/status/{initialDataFeedStatus}/finishexecution", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "finish data feed execution")
    public DataFeedExecution finishExecution(@PathVariable String customerSpace, //
            @PathVariable String datafeedName, @PathVariable String initialDataFeedStatus) {
        return datafeedService.finishExecution(customerSpace, datafeedName, initialDataFeedStatus);
    }

    @RequestMapping(value = "/{datafeedName}/status/{initialDataFeedStatus}/failexecution", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "fail data feed execution")
    public DataFeedExecution failExecution(@PathVariable String customerSpace, @PathVariable String datafeedName,
            @PathVariable String initialDataFeedStatus) {
        return datafeedService.failExecution(customerSpace, datafeedName, initialDataFeedStatus);
    }

    @RequestMapping(value = "/{datafeedName}/execution/workflow/{workflowId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "update data feed execution")
    public DataFeedExecution updateExecutionWorkflowId(@PathVariable String customerSpace,
            @PathVariable String datafeedName, @PathVariable Long workflowId) {
        return datafeedService.updateExecutionWorkflowId(customerSpace, datafeedName, workflowId);
    }

}
