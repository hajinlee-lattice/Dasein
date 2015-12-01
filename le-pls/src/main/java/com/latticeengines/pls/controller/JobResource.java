package com.latticeengines.pls.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.pls.Job;
import com.latticeengines.domain.exposed.pls.JobStatus;
import com.latticeengines.domain.exposed.pls.JobStep;
import com.latticeengines.domain.exposed.pls.JobStepType;
import com.latticeengines.domain.exposed.pls.JobType;
import com.latticeengines.domain.exposed.pls.StepStatus;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "jobs", description = "REST resource for jobs")
@RestController
@RequestMapping("/jobs")
@PreAuthorize("hasRole('View_PLS_Jobs')")
public class JobResource {

    // temporary stub data
    List<Job> jobs = new ArrayList<>();
    Job job = new Job();
    List<JobStep> steps = new ArrayList<>();
    {
        jobs.add(job);
        job.setId(1L);
        job.setJobStatus(JobStatus.RUNNING);
        job.setJobType(JobType.LOADFILE_IMPORT);
        job.setUser("LatticeUser1");
        job.setStartTimestamp(new Date());

        JobStep stepLoad = new JobStep();
        steps.add(stepLoad);
        stepLoad.setJobStepType(JobStepType.LOAD_DATA);
        stepLoad.setStepStatus(StepStatus.COMPLETED);
        stepLoad.setStartTimestamp(new Date());
        stepLoad.setEndTimestamp(new Date());

        JobStep stepMatch = new JobStep();
        steps.add(stepMatch);
        stepMatch.setJobStepType(JobStepType.MATCH_DATA);
        stepMatch.setStepStatus(StepStatus.COMPLETED);
        stepMatch.setStartTimestamp(new Date());
        stepMatch.setEndTimestamp(new Date());

        JobStep stepInsights = new JobStep();
        steps.add(stepInsights);
        stepInsights.setJobStepType(JobStepType.GENERATE_INSIGHTS);
        stepInsights.setStepStatus(StepStatus.COMPLETED);
        stepInsights.setStartTimestamp(new Date());
        stepInsights.setEndTimestamp(new Date());

        JobStep stepModel = new JobStep();
        steps.add(stepModel);
        stepModel.setJobStepType(JobStepType.CREATE_MODEL);
        stepModel.setStepStatus(StepStatus.COMPLETED);
        stepModel.setStartTimestamp(new Date());
        stepModel.setEndTimestamp(new Date());

        JobStep stepMarket = new JobStep();
        steps.add(stepMarket);
        stepMarket.setJobStepType(JobStepType.CREATE_GLOBAL_TARGET_MARKET);
        stepMarket.setStepStatus(StepStatus.RUNNING);
        stepMarket.setStartTimestamp(new Date());
    }

    @RequestMapping(value = "/{jobId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a job by id")
    @PreAuthorize("hasRole('View_PLS_Jobs')")
    public Job find(@PathVariable String jobId) {
        job.setSteps(steps);
        return job;
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    public List<Job> findAll() {
        job.setSteps(null);
        return jobs;
    }

    @RequestMapping(value = "/{jobId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Cancel a job")
    @PreAuthorize("hasRole('Edit_PLS_Jobs')")
    public void cancel(@PathVariable String jobId) {
        // TODO bernard
    }
}
