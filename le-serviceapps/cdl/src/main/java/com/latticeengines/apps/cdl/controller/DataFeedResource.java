package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafeeds", description = "REST resource for metadata data feeds")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datafeeds")
public class DataFeedResource {

    @Inject
    private DataFeedService datafeedService;

    @PostMapping(value = "/{datafeedName}/jobtype/{jobType}startexecution", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "start data feed execution")
    public DataFeedExecution startExecution(@PathVariable String customerSpace, //
            @PathVariable String datafeedName, //
            @PathVariable DataFeedExecutionJobType jobType, //
            @RequestBody long jobId) {
        return datafeedService.startExecution(customerSpace, datafeedName, jobType, jobId);
    }

    @PostMapping(value = "/{datafeedName}/jobtype/{jobType}/restartexecution", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "restart data feed execution")
    public Long restartExecution(@PathVariable String customerSpace, //
            @PathVariable String datafeedName, //
            @PathVariable DataFeedExecutionJobType jobType) {
        return datafeedService.restartExecution(customerSpace, datafeedName, jobType);
    }

    @PostMapping(value = "/{datafeedName}/jobtype/{jobType}/lockexecution", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "lock data feed execution")
    public ResponseDocument<Long> lockExecution(@PathVariable String customerSpace, //
                                                   @PathVariable String datafeedName, //
                                                   @PathVariable DataFeedExecutionJobType jobType) {
        return ResponseDocument.successResponse(datafeedService.lockExecution(customerSpace, datafeedName, jobType));
    }

    @GetMapping(value = "/{datafeedName}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "find data feed by name")
    public DataFeed findDataFeedByName(@PathVariable String customerSpace, @PathVariable String datafeedName) {
        return datafeedService.findDataFeedByName(customerSpace, datafeedName);
    }

    @PutMapping(value = "/{datafeedName}/status/{status}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "update data feed status by name")
    public void updateDataFeedStatus(@PathVariable String customerSpace, @PathVariable String datafeedName,
            @PathVariable String status) {
        datafeedService.updateDataFeed(customerSpace, datafeedName, status);
    }

    @PostMapping(value = "/{datafeedName}/status/{initialDataFeedStatus}/finishexecution",
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "finish data feed execution")
    public DataFeedExecution finishExecution(@PathVariable String customerSpace, //
                                             @PathVariable String datafeedName,
                                             @PathVariable String initialDataFeedStatus,
                                             @RequestParam(required = false) Long executionId) {
        if (StringUtils.isEmpty(executionId))
            return datafeedService.finishExecution(customerSpace, datafeedName, initialDataFeedStatus);
        return datafeedService.finishExecution(customerSpace, datafeedName, initialDataFeedStatus, executionId);
    }

    @PostMapping(value = "/{datafeedName}/status/{initialDataFeedStatus}/failexecution",
            headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "fail data feed execution")
    public DataFeedExecution failExecution(@PathVariable String customerSpace, @PathVariable String datafeedName,
                                           @PathVariable String initialDataFeedStatus,
                                           @RequestParam(required = false) Long executionId) {
        if (StringUtils.isEmpty(executionId))
            return datafeedService.failExecution(customerSpace, datafeedName, initialDataFeedStatus);
        return datafeedService.failExecution(customerSpace, datafeedName, initialDataFeedStatus, executionId);
    }

    @PostMapping(value = "/{datafeedName}/execution/workflow/{workflowId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "update data feed execution")
    public DataFeedExecution updateExecutionWorkflowId(@PathVariable String customerSpace,
            @PathVariable String datafeedName, @PathVariable Long workflowId) {
        return datafeedService.updateExecutionWorkflowId(customerSpace, datafeedName, workflowId);
    }

    @PostMapping(value = "/unblock/workflow/{workflowId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "unblock P&A job from submitting")
    public Boolean unblock(@PathVariable String customerSpace, @PathVariable Long workflowId) {
        return datafeedService.unblockPA(customerSpace, workflowId);
    }
}
