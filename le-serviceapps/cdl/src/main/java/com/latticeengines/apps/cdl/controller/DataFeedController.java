package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.workflow.EntityExportWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.OrphanRecordsExportWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.ProcessAnalyzeWorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.EntityExportRequest;
import com.latticeengines.domain.exposed.cdl.OrphanRecordsExportRequest;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "datafeeds", description = "Controller of data feed operations.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datacollection/datafeed")
public class DataFeedController {

    private final ProcessAnalyzeWorkflowSubmitter processAnalyzeWorkflowSubmitter;
    private final OrphanRecordsExportWorkflowSubmitter orphanRecordExportWorkflowSubmitter;
    private final EntityExportWorkflowSubmitter entityExportWorkflowSubmitter;

    @Inject
    public DataFeedController(ProcessAnalyzeWorkflowSubmitter processAnalyzeWorkflowSubmitter,
            OrphanRecordsExportWorkflowSubmitter orphanRecordExportWorkflowSubmitter,
                              EntityExportWorkflowSubmitter entityExportWorkflowSubmitter) {
        this.processAnalyzeWorkflowSubmitter = processAnalyzeWorkflowSubmitter;
        this.orphanRecordExportWorkflowSubmitter = orphanRecordExportWorkflowSubmitter;
        this.entityExportWorkflowSubmitter = entityExportWorkflowSubmitter;
    }

    @PostMapping(value = "/processanalyze", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke profile workflow. Returns the job id.")
    public ResponseDocument<String> processAnalyze(@PathVariable String customerSpace,
                                                   @RequestBody(required = false) ProcessAnalyzeRequest request) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        if (request == null) {
            request = defaultProcessAnalyzeRequest();
        }
        try {
            ApplicationId appId = processAnalyzeWorkflowSubmitter.submit(customerSpace, request,
                    new WorkflowPidWrapper(-1L));
            return ResponseDocument.successResponse(appId.toString());
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/processanalyze/restart", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Restart a previous failed processanalyze execution")
    public ResponseDocument<String> restart(@PathVariable String customerSpace,
                                            @ApiParam(value = "Memory in MB", required = false)
                                            @RequestParam(value = "memory", required = false) Integer memory) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        ApplicationId appId = processAnalyzeWorkflowSubmitter.retryLatestFailed(customerSpace, memory);
        return ResponseDocument.successResponse(appId.toString());
    }

    @PostMapping(value = "/exportorphanrecords", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke orphanRecordExport workflow. Returns the job id.")
    public ResponseDocument<String> orphanRecordExport(@PathVariable String customerSpace,
                                                       @RequestBody OrphanRecordsExportRequest request) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        try {
            ApplicationId appId = orphanRecordExportWorkflowSubmitter.submit(
                    customerSpace, request, new WorkflowPidWrapper(-1L));
            if (appId == null) {
                return null;
            }
            return ResponseDocument.successResponse(appId.toString());
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    @PostMapping(value = "/entityexport", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke profile workflow. Returns the job id.")
    public ResponseDocument<String> entityExport(@PathVariable String customerSpace,
                                                   @RequestBody EntityExportRequest request) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        try {
            ApplicationId appId = entityExportWorkflowSubmitter.submit(customerSpace, request,
                    new WorkflowPidWrapper(-1L));
            return ResponseDocument.successResponse(appId.toString());
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }

    private ProcessAnalyzeRequest defaultProcessAnalyzeRequest() {
        return new ProcessAnalyzeRequest();
    }

}
