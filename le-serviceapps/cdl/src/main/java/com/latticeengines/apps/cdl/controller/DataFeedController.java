package com.latticeengines.apps.cdl.controller;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.workflow.ConsolidateAndPublishWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.ProfileAndPublishWorkflowSubmitter;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.security.exposed.InternalResourceBase;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafeeds", description = "Controller of data feed operations.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datacollection/datafeed")
public class DataFeedController extends InternalResourceBase {

    private final ConsolidateAndPublishWorkflowSubmitter consolidateAndPublishWorkflowSubmitter;

    private final ProfileAndPublishWorkflowSubmitter profileAndPublishWorkflowSubmitter;

    @Inject
    public DataFeedController(ConsolidateAndPublishWorkflowSubmitter consolidateAndPublishWorkflowSubmitter,
            ProfileAndPublishWorkflowSubmitter profileAndPublishWorkflowSubmitter) {
        this.consolidateAndPublishWorkflowSubmitter = consolidateAndPublishWorkflowSubmitter;
        this.profileAndPublishWorkflowSubmitter = profileAndPublishWorkflowSubmitter;
    }

    @RequestMapping(value = "/consolidate", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke data feed consolidate workflow. Returns the job id.")
    public ResponseDocument<String> consolidate(@PathVariable String customerSpace, HttpServletRequest request) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        ApplicationId appId = consolidateAndPublishWorkflowSubmitter.submit(customerSpace);
        return ResponseDocument.successResponse(appId.toString());
    }

    @RequestMapping(value = "/consolidate/restart", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Restart a previous failed consolidate execution")
    public ResponseDocument<String> restart(@PathVariable String customerSpace, HttpServletRequest request) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        ApplicationId appId = consolidateAndPublishWorkflowSubmitter.retryLatestFailed(customerSpace);
        return ResponseDocument.successResponse(appId.toString());
    }

    @RequestMapping(value = "/profile", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Invoke profile workflow. Returns the job id.")
    public ResponseDocument<String> profile(@PathVariable String customerSpace, HttpServletRequest request) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        ApplicationId appId = profileAndPublishWorkflowSubmitter.submit(customerSpace);
        return ResponseDocument.successResponse(appId.toString());
    }
}
