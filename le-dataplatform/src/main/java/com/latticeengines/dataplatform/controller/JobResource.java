package com.latticeengines.dataplatform.controller;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;
import com.latticeengines.network.exposed.dataplatform.JobInterface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "jobs", description = "REST resource for all jobs")
@RestController
public class JobResource implements JobInterface {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(JobResource.class);

    @Autowired
    private JobService jobService;

    public JobResource() {
    }

    @RequestMapping(value = "/jobs/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get status about a submitted job")
    public JobStatus getJobStatus(@PathVariable String applicationId) {
        return jobService.getJobStatus(applicationId);
    }

    @RequestMapping(value = "/jobs/{applicationId}/counters", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get job counters for a completed mapreduce job")
    public Counters getMRJobCounters(@PathVariable String applicationId) {
        return jobService.getMRJobCounters(applicationId);
    }

}
