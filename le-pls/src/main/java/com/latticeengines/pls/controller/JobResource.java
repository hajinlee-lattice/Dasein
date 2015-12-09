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
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

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
    // @PreAuthorize("hasRole('View_PLS_Jobs')")
    public Job find(@PathVariable String jobId) {
        return workflowProxy.getWorkflowExecution(jobId);
    }

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    public List<Job> findAll() {
        Tenant tenant = SecurityContextUtils.getTenant();
        Tenant tenantWithPid = tenantEntityMgr.findByTenantId(tenant.getId());
        log.info("Finding jobs for " + tenantWithPid.toString() + " with pid " + tenantWithPid.getPid());
        if (workflowProxy == null) {
            log.info("restApiProxy is not set");
        }

        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid.getPid());
        if (jobs == null) {
            jobs = Collections.emptyList();
        }
        return jobs;
    }

    @RequestMapping(value = "/{jobId}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Cancel a job")
    // @PreAuthorize("hasRole('Edit_PLS_Jobs')")
    public void cancel(@PathVariable String jobId) {
        // TODO
    }
}
