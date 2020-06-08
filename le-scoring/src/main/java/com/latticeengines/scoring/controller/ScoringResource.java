package com.latticeengines.scoring.controller;

import java.util.Arrays;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.scoring.exposed.service.ScoringService;
import com.latticeengines.scoring.service.ScoringJobService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "scoring", description = "REST resource for scoring service by Lattice")
@RestController
@RequestMapping("/scoringjobs")
public class ScoringResource {

    @Inject
    private ScoringJobService scoringJobService;

    @Inject
    private ScoringService scoringService;

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Create a scoring job")
    public AppSubmission createScoringJob(@RequestBody ScoringConfiguration scoringConfig) {
        return new AppSubmission(Arrays.<ApplicationId> asList(scoringJobService.score(scoringConfig)));
    }

    @PostMapping("/rtsbulkscore")
    @ResponseBody
    @ApiOperation(value = "Submit a bulk scoring job")
    public AppSubmission submitBulkScoreJob(@RequestBody RTSBulkScoringConfiguration rtsBulkScoringConfig) {
        return new AppSubmission(
                Arrays.<ApplicationId> asList(scoringService.submitScoreWorkflow(rtsBulkScoringConfig)));
    }

}
