package com.latticeengines.pls.service.impl;

import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.service.JobService;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("jobService")
public class JobServiceImpl implements JobService {

    private static final Log log = LogFactory.getLog(JobServiceImpl.class);

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Override
    public AppSubmission restart(String jobId) {
        return workflowProxy.restartWorkflowExecution(jobId);
    }

    @Override
    public void cancel(String jobId) {
        workflowProxy.stopWorkflow(jobId);
    }

    @Override
    public List<Job> findAllWithType(String type) {
        Tenant tenantWithPid = getTenant();
        log.info("Finding jobs for " + tenantWithPid.toString() + " with pid "
                + tenantWithPid.getPid());
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid.getPid(), type);
        if (jobs == null) {
            jobs = Collections.emptyList();
        }
        return jobs;
    }

    @Override
    public Job findByApplicationId(String applicationId) {
        Tenant tenantWithPid = getTenant();
        log.info("Finding job for application Id " + applicationId + " with pid "
                + tenantWithPid.getPid());
        Job job = workflowProxy.getWorkflowJobFromApplicationId(applicationId);
        return job;
    }

    @Override
    public Job find(String jobId) {
        return workflowProxy.getWorkflowExecution(jobId);
    }

    @Override
    public List<Job> findAll() {
        Tenant tenantWithPid = getTenant();
        log.info("Finding jobs for " + tenantWithPid.toString() + " with pid "
                + tenantWithPid.getPid());
        List<Job> jobs = workflowProxy.getWorkflowExecutionsForTenant(tenantWithPid.getPid());
        if (jobs == null) {
            jobs = Collections.emptyList();
        }
        return jobs;
    }

    private Tenant getTenant() {
        Tenant tenant = MultiTenantContext.getTenant();
        return tenantEntityMgr.findByTenantId(tenant.getId());
    }

}
