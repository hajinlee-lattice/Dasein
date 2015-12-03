package com.latticeengines.pls.controller;

import java.util.Arrays;
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
    Job job = new Job();
    Job job1 = new Job();

    JobStep stepLoad = new JobStep();
    JobStep stepMatch = new JobStep();
    JobStep stepInsights = new JobStep();
    JobStep stepModel = new JobStep();
    JobStep stepMarket = new JobStep();
    JobStep stepMarketCompleted = new JobStep();
    
    {
        job.setId(1L);
        job.setJobStatus(JobStatus.RUNNING);
        job.setJobType(JobType.LOADFILE_IMPORT);
        job.setUser("LatticeUser1");
        job.setStartTimestamp(new Date());

        job1.setId(2L);
        job1.setJobStatus(JobStatus.COMPLETED);
        job1.setJobType(JobType.LOADFILE_IMPORT);
        job1.setUser("LatticeUser1");
        job1.setStartTimestamp(new Date());

        stepLoad.setJobStepType(JobStepType.LOAD_DATA);
        stepLoad.setStepStatus(StepStatus.COMPLETED);
        stepLoad.setStartTimestamp(new Date());
        stepLoad.setEndTimestamp(new Date());

        stepMatch.setJobStepType(JobStepType.MATCH_DATA);
        stepMatch.setStepStatus(StepStatus.COMPLETED);
        stepMatch.setStartTimestamp(new Date());
        stepMatch.setEndTimestamp(new Date());

        stepInsights.setJobStepType(JobStepType.GENERATE_INSIGHTS);
        stepInsights.setStepStatus(StepStatus.COMPLETED);
        stepInsights.setStartTimestamp(new Date());
        stepInsights.setEndTimestamp(new Date());

        stepModel.setJobStepType(JobStepType.CREATE_MODEL);
        stepModel.setStepStatus(StepStatus.COMPLETED);
        stepModel.setStartTimestamp(new Date());
        stepModel.setEndTimestamp(new Date());

        stepMarket.setJobStepType(JobStepType.CREATE_GLOBAL_TARGET_MARKET);
        stepMarket.setStepStatus(StepStatus.RUNNING);
        stepMarket.setStartTimestamp(new Date());

        stepMarketCompleted.setJobStepType(JobStepType.CREATE_GLOBAL_TARGET_MARKET);
        stepMarketCompleted.setStepStatus(StepStatus.COMPLETED);
        stepMarketCompleted.setStartTimestamp(new Date());
        stepMarketCompleted.setEndTimestamp(new Date());
    }

    @RequestMapping(value = "/{jobId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a job by id")
    @PreAuthorize("hasRole('View_PLS_Jobs')")
    public Job find(@PathVariable String jobId) {
        if (jobId.equals("1")) {
            job.setSteps(Arrays.asList(stepLoad, stepMatch, stepInsights, stepModel, stepMarket));
            return job;
        } else if (jobId.equals("2")) {
            job1.setSteps(Arrays.asList(stepLoad, stepMatch, stepInsights, stepModel, stepMarketCompleted));
            return job1;
        }
        return null;
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    public List<Job> findAll() {
        job.setSteps(Arrays.asList(stepLoad, stepMatch, stepInsights, stepModel, stepMarket));
        job1.setSteps(Arrays.asList(stepLoad, stepMatch, stepInsights, stepModel, stepMarketCompleted));
        return Arrays.asList(job, job1);
    }

    @RequestMapping(value = "/{jobId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Cancel a job")
    @PreAuthorize("hasRole('Edit_PLS_Jobs')")
    public void cancel(@PathVariable String jobId) {
        // TODO bernard
    }
}
