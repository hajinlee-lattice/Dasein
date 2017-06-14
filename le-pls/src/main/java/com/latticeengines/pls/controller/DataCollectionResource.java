package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.metadata.DataCollectionType;
import com.latticeengines.pls.workflow.CalculateStatsWorkflowSubmitter;
import com.latticeengines.pls.workflow.ConsolidateAndPublishWorkflowSubmitter;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "datacollection", description = "REST resource for interacting with data collection")
@RestController
@RequestMapping("/datacollections")
@PreAuthorize("hasRole('View_PLS_Data')")
public class DataCollectionResource {

    @Autowired
    private ConsolidateAndPublishWorkflowSubmitter consolidateAndPublishWorkflowSubmitter;

    @Autowired
    private CalculateStatsWorkflowSubmitter calculateStatsWorkflowSubmitter;

    @RequestMapping(value = "/{dataCollectionType}/datafeeds/{datafeedName}/consolidate", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Invoke data feed consolidate workflow. Returns the job id.")
    public ResponseDocument<String> consolidate(@PathVariable DataCollectionType dataCollectionType,
            @PathVariable String datafeedName) {
        return ResponseDocument.successResponse( //
                consolidateAndPublishWorkflowSubmitter.submit(dataCollectionType, datafeedName).toString());

    }

    @RequestMapping(value = "/{dataCollectionType}/datafeeds/{datafeedName}/calculatestats", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Invoke calculate stats workflow. Returns the job id.")
    public ResponseDocument<String> calculateStats(@PathVariable DataCollectionType dataCollectionType,
            @PathVariable String datafeedName) {
        return ResponseDocument
                .successResponse(calculateStatsWorkflowSubmitter.submit(dataCollectionType, datafeedName).toString());
    }
}
