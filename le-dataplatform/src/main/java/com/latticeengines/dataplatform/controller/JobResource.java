package com.latticeengines.dataplatform.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.aws.AwsApplicationId;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;
import com.latticeengines.network.exposed.dataplatform.JobInterface;
import com.latticeengines.yarn.exposed.service.JobService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "jobs", description = "REST resource for all jobs")
@RestController
public class JobResource implements JobInterface {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(JobResource.class);

    @Autowired
    private JobService jobService;

    public JobResource() {
    }

    @Override
    @RequestMapping(value = "/jobs/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status about a submitted job")
    public JobStatus getJobStatus(@PathVariable String applicationId) {
        if (!AwsApplicationId.isAwsBatchJob(applicationId)) {
            return jobService.getJobStatus(applicationId);
        } else {
            return jobService.getAwsBatchJobStatus(applicationId);
        }
    }

    @Override
    @RequestMapping(value = "/jobs/{applicationId}/counters", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get job counters for a completed mapreduce job")
    public Counters getMRJobCounters(@PathVariable String applicationId) {
        return jobService.getMRJobCounters(applicationId);
    }

}
