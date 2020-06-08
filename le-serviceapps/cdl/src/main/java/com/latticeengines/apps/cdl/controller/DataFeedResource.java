package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

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

    @PostMapping("/{datafeedName}/jobtype/{jobType}startexecution")
    @ResponseBody
    @ApiOperation(value = "start data feed execution")
    public DataFeedExecution startExecution(@PathVariable String customerSpace, //
            @PathVariable String datafeedName, //
            @PathVariable DataFeedExecutionJobType jobType, //
            @RequestBody long jobId) {
        return datafeedService.startExecution(customerSpace, datafeedName, jobType, jobId);
    }

    @PostMapping("/{datafeedName}/jobtype/{jobType}/restartexecution")
    @ResponseBody
    @ApiOperation(value = "restart data feed execution")
    public Long restartExecution(@PathVariable String customerSpace, //
            @PathVariable String datafeedName, //
            @PathVariable DataFeedExecutionJobType jobType) {
        return datafeedService.restartExecution(customerSpace, datafeedName, jobType);
    }

    @PostMapping("/{datafeedName}/jobtype/{jobType}/lockexecution")
    @ResponseBody
    @ApiOperation(value = "lock data feed execution")
    public ResponseDocument<Long> lockExecution(@PathVariable String customerSpace, //
                                                   @PathVariable String datafeedName, //
                                                   @PathVariable DataFeedExecutionJobType jobType) {
        return ResponseDocument.successResponse(datafeedService.lockExecution(customerSpace, datafeedName, jobType));
    }

    @GetMapping("/{datafeedName}")
    @ResponseBody
    @ApiOperation(value = "find data feed by name")
    public DataFeed findDataFeedByName(@PathVariable String customerSpace, @PathVariable String datafeedName) {
        return datafeedService.findDataFeedByName(customerSpace, datafeedName);
    }

    @PutMapping("/{datafeedName}/status/{status}")
    @ResponseBody
    @ApiOperation(value = "update data feed status by name")
    public void updateDataFeedStatus(@PathVariable String customerSpace, @PathVariable String datafeedName,
            @PathVariable String status) {
        datafeedService.updateDataFeed(customerSpace, datafeedName, status);
    }

    @PostMapping("/{datafeedName}/status/{initialDataFeedStatus}/finishexecution")
    @ResponseBody
    @ApiOperation(value = "finish data feed execution")
    public DataFeedExecution finishExecution(@PathVariable String customerSpace, //
                                             @PathVariable String datafeedName,
                                             @PathVariable String initialDataFeedStatus,
                                             @RequestParam(required = false) Long executionId) {
        if (executionId == null) {
            return datafeedService.finishExecution(customerSpace, datafeedName, initialDataFeedStatus);
        } else {
            return datafeedService.finishExecution(customerSpace, datafeedName, initialDataFeedStatus, executionId);
        }
    }

    @PostMapping("/{datafeedName}/status/{initialDataFeedStatus}/failexecution")
    @ResponseBody
    @ApiOperation(value = "fail data feed execution")
    public DataFeedExecution failExecution(@PathVariable String customerSpace, @PathVariable String datafeedName,
                                           @PathVariable String initialDataFeedStatus,
                                           @RequestParam(required = false) Long executionId) {
        if (executionId == null) {
            return datafeedService.failExecution(customerSpace, datafeedName, initialDataFeedStatus);
        } else {
            return datafeedService.failExecution(customerSpace, datafeedName, initialDataFeedStatus, executionId);
        }
    }

    @PostMapping("/{datafeedName}/execution/workflow/{workflowId}")
    @ResponseBody
    @ApiOperation(value = "update data feed execution")
    public DataFeedExecution updateExecutionWorkflowId(@PathVariable String customerSpace,
            @PathVariable String datafeedName, @PathVariable Long workflowId) {
        return datafeedService.updateExecutionWorkflowId(customerSpace, datafeedName, workflowId);
    }

    @PostMapping("/unblock/workflow/{workflowId}")
    @ResponseBody
    @ApiOperation(value = "unblock P&A job from submitting")
    public Boolean unblock(@PathVariable String customerSpace, @PathVariable Long workflowId) {
        return datafeedService.unblockPA(customerSpace, workflowId);
    }
}
