package com.latticeengines.workflowapi.service.impl;

import java.util.List;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
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
        try {
            return JobStatus.getMappedStatus(workflowJobEntityMgr.findByWorkflowId(workflowId).getStatus());
        } catch (RuntimeException exc) {
            log.warn(String.format("Failed to get job status for job %d.", workflowId));

            return null;
        }
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
        try {
            Tenant tenant = tenantService.findByTenantId(CustomerSpace.parse(customerSpace).toString());
            List<WorkflowJob> jobs = (workflowIds == null) ?
                    workflowJobEntityMgr.findByTenant(tenant) :
                    workflowJobEntityMgr.findByTenantAndWorkflowIds(tenant, workflowIds);
            jobs.removeIf(Objects::isNull);
            return jobs.stream().map(job -> JobStatus.getMappedStatus(job.getStatus())).collect(Collectors.toList());
        } catch (RuntimeException exc) {
            log.warn(String.format("Failed to get job status. customerSpace=%s, workflowIds=%s.",
                    customerSpace, JsonUtils.serialize(workflowIds)));

            return Collections.emptyList();
        }
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
    public List<Job> getJobs(String customerSpace, Set<Long> workflowIds, Set<String> types, Boolean includeDetails,
                             Boolean hasParentId, Long parentJobId) {
        try {
            Tenant tenant = tenantService.findByTenantId(CustomerSpace.parse(customerSpace).toString());
            List<Job> jobs = getJobsByTenant(tenant.getPid());
            jobs.removeIf(Objects::isNull);

            if (workflowIds != null) {
                jobs.removeIf(job -> !workflowIds.contains(job.getId()));
            }

            if (types != null) {
                jobs.removeIf(job -> !types.contains(job.getJobType()));
            }

            if (!includeDetails) {
                jobs.forEach(job -> job.setSteps(null));
            }

            if (hasParentId) {
                jobs.removeIf(job -> !parentJobId.equals(job.getParentId()));
            }

            return jobs;
        } catch (RuntimeException exc) {
            log.warn(String.format("Failed to get jobs. customerSpace=%s, workflowIds=%s, types=%s, " +
                    "includeDetails=%s, hasParentId=%s, parentJobId=%d",
                    customerSpace, JsonUtils.serialize(workflowIds), JsonUtils.serialize(types),
                    String.valueOf(includeDetails), String.valueOf(hasParentId), parentJobId));

            return Collections.emptyList();
        }
    }

    @Override
    public List<Job> getJobsByTenant(Long tenantPid) {
        return workflowContainerService.getJobsByTenant(tenantPid);
    }

    @Override
    public List<Job> getJobsByTenant(Long tenantPid, List<String> types) {
        try {
            List<Job> jobs = workflowContainerService.getJobsByTenant(tenantPid);
            jobs.removeIf(Objects::isNull);

            if (types != null) {
                types.forEach(type -> jobs.removeIf(job -> !job.getJobType().equals(type)));
            }

            return jobs;
        } catch (RuntimeException exc) {
            log.warn(String.format("Failed to get jobs. tenantPid=%d, types=%s",
                    tenantPid, JsonUtils.serialize(types)));

            return Collections.emptyList();
        }
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

    @Override
    public void updateParentJobId(String customerSpace, List<Long> workflowIds, Long parentJobId) {
        Tenant tenant = tenantService.findByTenantId(CustomerSpace.parse(customerSpace).toString());
        List<WorkflowJob> jobs = workflowJobEntityMgr.findByTenantAndWorkflowIds(tenant, workflowIds);
        jobs.removeIf(Objects::isNull);
        jobs.forEach(job -> {
            job.setParentJobId(parentJobId);
            workflowJobEntityMgr.update(job);
        });
    }

    @VisibleForTesting
    void setWorkflowService(WorkflowService workflowService) {
        this.workflowService = workflowService;
    }

    @VisibleForTesting
    void setWorkflowContainerService(WorkflowContainerService workflowContainerService) {
        this.workflowContainerService = workflowContainerService;
    }
}
