package com.latticeengines.scoring.controller;

import java.util.Arrays;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.scoring.exposed.service.ScoringService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "scoring", description = "REST resource for scoring service by Lattice")
@RestController
@RequestMapping("/scoringjobs")
public class ScoringResource {

    @Autowired
    private ScoringService scoringService;

    @RequestMapping(value = "/rtsbulkscore", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Submit a bulk scoring job")
    public AppSubmission submitBulkScoreJob(@RequestBody RTSBulkScoringConfiguration rtsBulkScoringConfig) {
        return new AppSubmission(
                Arrays.<ApplicationId> asList(scoringService.submitScoreWorkflow(rtsBulkScoringConfig)));
    }

}
