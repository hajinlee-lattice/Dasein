package com.latticeengines.workflowapi.service.impl;

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflowapi.service.WorkflowContainerService;
import com.latticeengines.workflowapi.service.WorkflowJobService;

@Component("workflowApiWorkflowJobService")
public class WorkflowJobServiceImpl implements WorkflowJobService {
    private static final Logger log = LoggerFactory.getLogger(WorkflowJobServiceImpl.class);

    @Value("${hadoop.yarn.timeline-service.webapp.address}")
    private String timelineServiceUrl;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private WorkflowContainerService workflowContainerService;

    @Autowired
    private TenantService tenantService;

    @Override
    public JobStatus getJobStatus(Long workflowId) {
        return JobStatus.getMappedStatus(workflowJobEntityMgr.findByWorkflowId(workflowId).getStatus());
    }

    @Override
    public List<JobStatus> getJobStatus(List<Long> workflowIds) {
        List<JobStatus> status = new ArrayList<>();
        for (Long workflowId : workflowIds) {
            status.add(getJobStatus(workflowId));
        }

        return status;
    }

    @Override
    public List<JobStatus> getJobStatus(String customerSpace, List<Long> workflowIds) {
        Tenant tenant = tenantService.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        List<WorkflowJob> jobs = workflowJobEntityMgr.findByTenant(tenant);
        if (workflowIds != null) {
            workflowIds.forEach(workflowId -> jobs.removeIf(job -> !job.getWorkflowId().equals(workflowId)));
        }
        return jobs.stream().map(job -> JobStatus.getMappedStatus(job.getStatus())).collect(Collectors.toList());
    }

    @Override
    public Job getJob(Long workflowId) {
        return workflowService.getJob(new WorkflowExecutionId(workflowId));
    }

    @Override
    public List<Job> getJobs(List<Long> workflowIds) {
        return workflowService.getJobs(workflowIds.stream().map(WorkflowExecutionId::new).collect(Collectors.toList()));
    }

    @Override
    public List<Job> getJobs(List<Long> workflowIds, String type) {
        return workflowService.getJobs(
                workflowIds.stream().map(WorkflowExecutionId::new).collect(Collectors.toList()), type);
    }

    @Override
    public List<Job> getJobs(String customerSpace, List<Long> workflowIds, List<String> types, Boolean includeDetails,
                             Boolean hasParentId, Long parentJobId) {
        Tenant tenant = tenantService.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        List<Job> jobs = getJobsByTenant(tenant.getPid());

        if (workflowIds != null) {
            workflowIds.forEach(workflowId -> jobs.removeIf(job -> !job.getId().equals(workflowId)));
        }

        if (types != null) {
            types.forEach(type -> jobs.removeIf(job -> !job.getJobType().equals(type)));
        }

        if (!includeDetails) {
            jobs.forEach(job -> job.setSteps(null));
        }

        if (hasParentId) {
            jobs.removeIf(job -> !job.getParentId().equals(parentJobId));
        }

        return jobs;
    }

    @Override
    public List<Job> getJobsByTenant(Long tenantPid) {
        return workflowContainerService.getJobsByTenant(tenantPid);
    }

    @Override
    public List<Job> getJobsByTenant(Long tenantPid, boolean included, List<String> types) {
        List<Job> jobs = workflowContainerService.getJobsByTenant(tenantPid);

        if (types != null) {
            types.forEach(type -> jobs.removeIf(job -> !job.getJobType().equals(type)));
        }

        return jobs;
    }

    @Override
    public JobStatus getJobStatusByApplicationId(String applicationId) {
        return JobStatus.getMappedStatus(workflowJobEntityMgr.findByApplicationId(applicationId).getStatus());
    }

    @Override
    public List<String> getStepNames(WorkflowExecutionId workflowId) {
        return workflowService.getStepNames(workflowId);
    }

    @Override
    public String getWorkflowName(JobExecution jobExecution) {
        return jobExecution.getJobInstance().getJobName();
    }
}
