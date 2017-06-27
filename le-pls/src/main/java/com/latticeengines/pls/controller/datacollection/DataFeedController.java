package com.latticeengines.pls.controller.datacollection;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.pls.workflow.CalculateStatsWorkflowSubmitter;
import com.latticeengines.pls.workflow.ConsolidateAndPublishWorkflowSubmitter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datafeeds", description = "Controller of data feed operations.")
@RestController
@RequestMapping("/datacollection/datafeeds")
@PreAuthorize("hasRole('View_PLS_Data')")
public class DataFeedController {

    @Autowired
    private ConsolidateAndPublishWorkflowSubmitter consolidateAndPublishWorkflowSubmitter;

    @Autowired
    private CalculateStatsWorkflowSubmitter calculateStatsWorkflowSubmitter;

    @RequestMapping(value = "/{datafeedName}/consolidate", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Invoke data feed consolidate workflow. Returns the job id.")
    public ResponseDocument<String> consolidate(@PathVariable String datafeedName) {
        return ResponseDocument.successResponse( //
                consolidateAndPublishWorkflowSubmitter.submit(datafeedName).toString());
    }

    @RequestMapping(value = "/{datafeedName}/consolidate/restart", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Restart a previous failed consolidate execution")
    public ResponseDocument<String> restart(@PathVariable String datafeedName) {
        return ResponseDocument
                .successResponse(consolidateAndPublishWorkflowSubmitter.retryLatestFailed(datafeedName).toString());
    }

    @RequestMapping(value = "/{datafeedName}/assemble", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Invoke calculate stats workflow. Returns the job id.")
    public ResponseDocument<String> assemble(@PathVariable String datafeedName) {
        return ResponseDocument.successResponse(calculateStatsWorkflowSubmitter.submit(datafeedName).toString());
    }
}
