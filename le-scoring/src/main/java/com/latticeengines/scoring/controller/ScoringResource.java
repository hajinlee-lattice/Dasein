package com.latticeengines.scoring.controller;

import java.util.Arrays;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.scoring.service.ScoringJobService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "scoring", description = "REST resource for scoring service by Lattice")
@RestController
@RequestMapping("/scoringjobs")
public class ScoringResource {

    @Autowired
    private ScoringJobService scoringJobService;

    @Autowired
    private JobService jobService;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a scoring job")
    public AppSubmission createScoringJob(@RequestBody ScoringConfiguration scoringConfig) {
        return new AppSubmission(Arrays.<ApplicationId> asList(scoringJobService.score(scoringConfig)));
    }

    @RequestMapping(value = "/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status for submitted import job")
    public JobStatus getImportDataJobStatus(@PathVariable String applicationId) {
        return jobService.getJobStatus(applicationId);
    }
}
