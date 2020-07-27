package com.latticeengines.dataplatform.controller;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.aws.AwsApplicationId;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;
import com.latticeengines.yarn.exposed.service.JobService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "REST resource for all jobs")
@RestController
public class JobResource {

    @Inject
    private JobService jobService;

    @GetMapping("/jobs/{applicationId}")
    @ResponseBody
    @ApiOperation(value = "Get status about a submitted job")
    public JobStatus getJobStatus(@PathVariable String applicationId) {
        if (!AwsApplicationId.isAwsBatchJob(applicationId)) {
            return jobService.getJobStatus(applicationId);
        } else {
            return jobService.getAwsBatchJobStatus(applicationId);
        }
    }

    @GetMapping("/jobs/{applicationId}/counters")
    @ResponseBody
    @ApiOperation(value = "Get job counters for a completed mapreduce job")
    public Counters getMRJobCounters(@PathVariable String applicationId) {
        return jobService.getMRJobCounters(applicationId);
    }

}
