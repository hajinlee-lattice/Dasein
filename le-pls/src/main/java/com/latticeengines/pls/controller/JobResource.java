package com.latticeengines.pls.controller;

import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "jobs", description = "REST resource for jobs")
@RestController
@RequestMapping("/jobs")
@PreAuthorize("hasRole('View_PLS_Jobs')")
public class JobResource {

    private static final Log log = LogFactory.getLog(JobResource.class);

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @RequestMapping(value = "/{jobId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a job by id")
    @PreAuthorize("hasRole('View_PLS_Jobs')")
    public Job find(@PathVariable String jobId) {
        return workflowProxy.getWorkflowExecution(jobId);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve all jobs")
    public List<Job> findAll() {
        Tenant tenantWithPid = getTenant();
        log.info("Finding jobs for " + tenantWithPid.toString() + " with pid " + tenantWithPid.getPid());
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid.getPid());
        if (jobs == null) {
            jobs = Collections.emptyList();
        }
        return jobs;
    }

    @RequestMapping(value = "/yarnapps/{applicationId}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Retrieve job from yarn application id")
    public Job findByApplicationId(@PathVariable String applicationId) {
        Tenant tenantWithPid = getTenant();
        log.info("Finding job for application Id " + applicationId + " with pid " + tenantWithPid.getPid());

        Job job = workflowProxy.getWorkflowJobFromApplicationId(applicationId);
        return job;
    }

    @RequestMapping(value = "/find", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Find jobs with the provided job type")
    public List<Job> findAllWithType(@RequestParam("type") String type) {
        Tenant tenantWithPid = getTenant();
        log.info("Finding jobs for " + tenantWithPid.toString() + " with pid " + tenantWithPid.getPid());
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid.getPid(), type);
        if (jobs == null) {
            jobs = Collections.emptyList();
        }
        return jobs;
    }

    private Tenant getTenant() {
        Tenant tenant = SecurityContextUtils.getTenant();
        return tenantEntityMgr.findByTenantId(tenant.getId());
    }

    @RequestMapping(value = "/{jobId}/cancel", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Cancel a running job")
    @PreAuthorize("hasRole('Edit_PLS_Jobs')")
    public void cancel(@PathVariable String jobId) {
        workflowProxy.stopWorkflow(jobId);
    }

    @RequestMapping(value = "/{jobId}/restart", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Restart a previous job")
    @PreAuthorize("hasRole('Edit_PLS_Jobs')")
    public AppSubmission restart(@PathVariable String jobId) {
        return workflowProxy.restartWorkflowExecution(jobId);
    }
}
