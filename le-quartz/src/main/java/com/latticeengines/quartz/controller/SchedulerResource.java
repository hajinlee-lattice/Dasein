package com.latticeengines.quartz.controller;

import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.quartz.JobConfig;
import com.latticeengines.domain.exposed.quartz.JobInfo;
import com.latticeengines.domain.exposed.quartz.JobInfoDetail;
import com.latticeengines.quartz.entitymanager.SchedulerEntityMgr;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "quartz", description = "REST resource for quartz")
@RestController
@RequestMapping("/scheduler")
public class SchedulerResource {

    @Autowired
    private SchedulerEntityMgr schedulerEntityMgr;

    @RequestMapping(value = "/status", method = RequestMethod.POST)
    @ResponseBody
    @ApiOperation(value = "Set quartz scheduler status")
    public Boolean setSchedulerStatus(@RequestParam(value = "status") String status, HttpServletRequest request) {
        switch (status) {
        case "Pause":
            return schedulerEntityMgr.pauseAllJobs();
        case "Resume":
            return schedulerEntityMgr.resumeAllJobs();
        default:
            return false;
        }
    }

    @RequestMapping(value = "/jobs/{tenantId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add a job for certain tenant")
    public Boolean addJob(@PathVariable String tenantId, @RequestBody JobConfig jobConfig, HttpServletRequest request) {
        return schedulerEntityMgr.addJob(tenantId, jobConfig);
    }

    @RequestMapping(value = "/predefined/jobs/{jobName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add a job for certain tenant")
    public Boolean addPredefinedJob(@PathVariable String jobName, HttpServletRequest request) {
        return schedulerEntityMgr.addPredefinedJob(jobName);
    }

    @RequestMapping(value = "/jobs/{tenantId}/{jobName}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete a certain job with tenantId & job name")
    public Boolean deleteJob(@PathVariable String tenantId, @PathVariable(value = "jobName") String jobName,
            HttpServletRequest request) {
        return schedulerEntityMgr.deleteJob(tenantId, jobName);
    }

    @RequestMapping(value = "/jobs/{tenantId}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get jobs for certain tenant")
    public List<JobInfo> listJobs(@PathVariable String tenantId, HttpServletRequest request) {
        return schedulerEntityMgr.listJobs(tenantId);
    }

    @RequestMapping(value = "/jobs", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get all jobs in scheduler")
    public List<JobInfo> listAllJobs(HttpServletRequest request) {
        return schedulerEntityMgr.listAllJobs();
    }

    @RequestMapping(value = "/jobs/{tenantId}/{jobName}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get certain job details")
    public JobInfoDetail getJobDetail(@PathVariable String tenantId, @PathVariable String jobName,
            HttpServletRequest request) {
        return schedulerEntityMgr.getJobDetail(tenantId, jobName);
    }

    @RequestMapping(value = "/nextDate", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get next date from cron schedule")
    public Date getNextDateFromCronExpression(@PathVariable String cronExpression, HttpServletRequest request) {
        return schedulerEntityMgr.getNextDateFromCronExpression(cronExpression);
    }

}
