package com.latticeengines.apps.cdl.controller;

import java.util.Date;

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

import com.latticeengines.apps.cdl.service.CDLJobService;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

/**
 * This controller assumes operation on the default datafeed in default
 * datacollection
 */
@Api(value = "datafeed", description = "REST resource for default metadata data feed")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datafeed")
public class DefaultDataFeedController {

    @Inject
    private DataFeedService datafeedService;

    @Inject
    private CDLJobService cdlJobService;

    @GetMapping
    @ResponseBody
    @ApiOperation(value = "find data feed by name")
    public DataFeed findDataFeedByName(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.getOrCreateDataFeed(customerSpace);
    }

    @GetMapping("/default")
    @ResponseBody
    @ApiOperation(value = "find data feed by name")
    public DataFeed getDefaultDataFeed(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.getDefaultDataFeed(customerSpace);
    }

    @PutMapping("/drainingstatus/{drainingStatus}")
    @ResponseBody
    @ApiOperation(value = "update data feed status by name")
    public void updateDataFeedDrainingStatus(@PathVariable String customerSpace, @PathVariable String drainingStatus) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        datafeedService.updateDataFeedDrainingStatus(customerSpace, drainingStatus);
    }

    @PutMapping("/maintenance/{maintenanceMode}")
    @ResponseBody
    @ApiOperation(value = "update data feed status by name")
    public void updateDataFeedMaintenanceMode(@PathVariable String customerSpace,
            @PathVariable boolean maintenanceMode) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        datafeedService.updateDataFeedMaintenanceMode(customerSpace, maintenanceMode);
    }

    @PostMapping("/jobtype/{jobType}/startexecution")
    @ResponseBody
    @ApiOperation(value = "start data feed execution")
    public DataFeedExecution startExecution(@PathVariable String customerSpace, //
            @PathVariable DataFeedExecutionJobType jobType, //
            @RequestBody long jobId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.startExecution(customerSpace, "", jobType, jobId);
    }

    @PostMapping("/jobtype/{jobType}/restartexecution")
    @ResponseBody
    @ApiOperation(value = "restart data feed execution")
    public Long restartExecution(@PathVariable String customerSpace, //
            @PathVariable DataFeedExecutionJobType jobType) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.restartExecution(customerSpace, "", jobType);
    }

    @PostMapping("/jobtype/{jobType}/lockexecution")
    @ResponseBody
    @ApiOperation(value = "lock data feed execution")
    public ResponseDocument<Long> lockExecution(@PathVariable String customerSpace, //
            @PathVariable DataFeedExecutionJobType jobType) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return ResponseDocument.successResponse(datafeedService.lockExecution(customerSpace, "", jobType));
    }

    @PutMapping("/status/{status}")
    @ResponseBody
    @ApiOperation(value = "update data feed status by name")
    public void updateDataFeedStatus(@PathVariable String customerSpace, @PathVariable String status) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        datafeedService.updateDataFeed(customerSpace, "", status);
    }

    @PostMapping("/updatenextinvoketime")
    @ResponseBody
    @ApiOperation(value = "update data feed next invoke time by name")
    public void updateDataFeedNextInvokeTime(@PathVariable String customerSpace, @RequestBody(required = false) Date time) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        datafeedService.updateDataFeedNextInvokeTime(customerSpace, time);
    }

    @PostMapping("/updatescheduletime")
    @ResponseBody
    @ApiOperation(value = "update data feed schedule time by name")
    public void updateDataFeedScheduleTime(@PathVariable String customerSpace,
                                           @RequestParam Boolean scheduleNow,
                                           @RequestBody(required = false) ProcessAnalyzeRequest request) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        datafeedService.updateDataFeedScheduleTime(customerSpace, scheduleNow, request);
    }


    @GetMapping("/jobtype/{jobType}/latestexecution")
    @ResponseBody
    @ApiOperation(value = "get the latest data feed execution")
    public DataFeedExecution getLatestExecution(@PathVariable String customerSpace, //
            @PathVariable DataFeedExecutionJobType jobType) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        DataFeed dataFeed = datafeedService.getDefaultDataFeed(customerSpace);
        return datafeedService.getLatestExecution(customerSpace, dataFeed.getName(), jobType);
    }

    @PostMapping("/status/{initialDataFeedStatus}/finishexecution")
    @ResponseBody
    @ApiOperation(value = "finish data feed execution")
    public DataFeedExecution finishExecution(@PathVariable String customerSpace,
                                             @PathVariable String initialDataFeedStatus,
                                             @RequestParam(required = false) Long executionId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        if (executionId == null) {
            return datafeedService.finishExecution(customerSpace, "", initialDataFeedStatus);
        } else {
            return datafeedService.finishExecution(customerSpace, "", initialDataFeedStatus, executionId);
        }
    }

    @PostMapping("/status/{initialDataFeedStatus}/failexecution")
    @ResponseBody
    @ApiOperation(value = "fail data feed execution")
    public DataFeedExecution failExecution(@PathVariable String customerSpace,
            @PathVariable String initialDataFeedStatus, @RequestParam(required = false) Long executionId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        if (executionId == null) {
            return datafeedService.failExecution(customerSpace, "", initialDataFeedStatus);
        } else {
            return datafeedService.failExecution(customerSpace, "", initialDataFeedStatus, executionId);
        }
    }

    @PostMapping("/execution/workflow/{workflowId}")
    @ResponseBody
    @ApiOperation(value = "update data feed execution")
    public DataFeedExecution updateExecutionWorkflowId(@PathVariable String customerSpace,
            @PathVariable Long workflowId) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.updateExecutionWorkflowId(customerSpace, "", workflowId);
    }

    @PostMapping("/rebuildtransaction/{isRebuild}")
    @ResponseBody
    @ApiOperation(value = "rebuild transaction store")
    public DataFeed rebuildTransaction(@PathVariable String customerSpace, @PathVariable Boolean isRebuild) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.rebuildTransaction(customerSpace, "", isRebuild);
    }

    @PostMapping("/earliesttransaction/{earliestDayPeriod}/{latestDayPeriod}")
    @ResponseBody
    @ApiOperation(value = "update earliest and latest transaction day period")
    public DataFeed updateEarliestLatestTransaction(@PathVariable String customerSpace,
            @PathVariable Integer earliestDayPeriod, @PathVariable Integer latestDayPeriod) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return datafeedService.updateEarliestLatestTransaction(customerSpace, "", earliestDayPeriod, latestDayPeriod);
    }

    @GetMapping("/nextinvoketime")
    @ResponseBody
    @ApiOperation(value = "Get tentative next invoke time of scheduled P&A")
    public Long nextInvokeTime(@PathVariable String customerSpace) {
        Date invokeTime = cdlJobService.getNextInvokeTime(CustomerSpace.parse(customerSpace));
        return invokeTime != null ? invokeTime.getTime() : null;
    }

}
