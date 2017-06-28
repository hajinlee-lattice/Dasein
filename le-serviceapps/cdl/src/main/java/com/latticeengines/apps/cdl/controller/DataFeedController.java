package com.latticeengines.apps.cdl.controller;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.workflow.ConsolidateAndPublishWorkflowSubmitter;
import com.latticeengines.apps.cdl.workflow.ProfileAndPublishWorkflowSubmitter;
import com.latticeengines.domain.exposed.ResponseDocument;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafeeds", description = "Controller of data feed operations.")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/datacollection/datafeed")
public class DataFeedController {

    @Autowired
    private ConsolidateAndPublishWorkflowSubmitter consolidateAndPublishWorkflowSubmitter;

    @Autowired
    private ProfileAndPublishWorkflowSubmitter profileAndPublishWorkflowSubmitter;

    @RequestMapping(value = "/consolidate", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Invoke data feed consolidate workflow. Returns the job id.")
    public ResponseDocument<String> consolidate(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return ResponseDocument.successResponse( //
                consolidateAndPublishWorkflowSubmitter.submit(customerSpace).toString());
    }


    @RequestMapping(value = "/consolidate/restart", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Restart a previous failed consolidate execution")
    public ResponseDocument<String> restart(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return ResponseDocument
                .successResponse(consolidateAndPublishWorkflowSubmitter.retryLatestFailed(customerSpace).toString());
    }


    @RequestMapping(value = "/profile", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Invoke profile workflow. Returns the job id.")
    public ResponseDocument<String> profile(@PathVariable String customerSpace) {
        customerSpace = CustomerSpace.parse(customerSpace).toString();
        return ResponseDocument
                .successResponse(profileAndPublishWorkflowSubmitter.submit(customerSpace).toString());
    }
}
