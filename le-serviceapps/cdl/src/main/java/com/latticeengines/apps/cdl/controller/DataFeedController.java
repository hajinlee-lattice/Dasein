package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.workflow.ConsolidateAndPublishWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.ProcessAnalyzeWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.ProfileAndPublishWorkflowSubmitter;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafeeds", description = "Controller of data feed operations.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datacollection/datafeed")
public class DataFeedController {

    private final ConsolidateAndPublishWorkflowSubmitter consolidateAndPublishWorkflowSubmitter;

    private final ProfileAndPublishWorkflowSubmitter profileAndPublishWorkflowSubmitter;

    private final ProcessAnalyzeWorkflowSubmitter processAnalyzeWorkflowSubmitter;

    @Inject
    public DataFeedController(ConsolidateAndPublishWorkflowSubmitter consolidateAndPublishWorkflowSubmitter,
            ProfileAndPublishWorkflowSubmitter profileAndPublishWorkflowSubmitter,
            ProcessAnalyzeWorkflowSubmitter processAnalyzeWorkflowSubmitter) {
        this.consolidateAndPublishWorkflowSubmitter = consolidateAndPublishWorkflowSubmitter;
        this.profileAndPublishWorkflowSubmitter = profileAndPublishWorkflowSubmitter;
        this.processAnalyzeWorkflowSubmitter = processAnalyzeWorkflowSubmitter;
    }

    @RequestMapping(value = "/consolidate", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke data feed consolidate workflow. Returns the job id.")
    public ResponseDocument<String> consolidate(@PathVariable String customerSpace,
            @RequestParam(value = "draining", required = false, defaultValue = "false") boolean draining) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        ApplicationId appId = consolidateAndPublishWorkflowSubmitter.submit(customerSpace, draining);
        return ResponseDocument.successResponse(appId.toString());
    }

    @RequestMapping(value = "/consolidate/restart", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Restart a previous failed consolidate execution")
    public ResponseDocument<String> restart(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        ApplicationId appId = consolidateAndPublishWorkflowSubmitter.retryLatestFailed(customerSpace);
        return ResponseDocument.successResponse(appId.toString());
    }

    @RequestMapping(value = "/profile", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke profile workflow. Returns the job id.")
    public ResponseDocument<String> profile(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        ApplicationId appId = profileAndPublishWorkflowSubmitter.submit(customerSpace);
        return ResponseDocument.successResponse(appId.toString());
    }

    @RequestMapping(value = "/processanalyze", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke profile workflow. Returns the job id.")
    public ResponseDocument<String> processAnalyze(@PathVariable String customerSpace, //
                                                   @RequestBody(required = false) ProcessAnalyzeRequest request) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        ApplicationId appId = processAnalyzeWorkflowSubmitter.submit(customerSpace);
        return ResponseDocument.successResponse(appId.toString());
    }
}
