package com.latticeengines.workflowapi.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowProperty;
import com.latticeengines.proxy.exposed.dataplatform.JobProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.exposed.service.WorkflowTenantService;
import com.latticeengines.workflow.exposed.user.WorkflowUser;
import com.latticeengines.workflow.exposed.util.WorkflowUtils;
import com.latticeengines.workflowapi.service.WorkflowContainerService;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.yarn.exposed.service.JobService;

@Component("workflowContainerService")
public class WorkflowContainerServiceImpl implements WorkflowContainerService {

    private static final Log log = LogFactory.getLog(WorkflowContainerService.class);

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private JobService jobService;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private WorkflowTenantService workflowTenantService;

    @Autowired
    private WorkflowJobEntityMgr workflowEntityMgr;

    @Autowired
    private WorkflowService workflowService;

    @Autowired
    private JobProxy jobProxy;

    @Override
    public ApplicationId submitWorkFlow(WorkflowConfiguration workflowConfig) {
        Job job = createJob(workflowConfig);
        ApplicationId appId = jobService.submitJob(job);
        job.setId(appId.toString());
        jobEntityMgr.create(job);

        Tenant tenant = workflowTenantService.getTenantFromConfiguration(workflowConfig);
        String user = workflowConfig.getUserId();
        user = user != null ? user : WorkflowUser.DEFAULT_USER.name();

        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setTenant(tenant);
        workflowJob.setUserId(user);
        workflowJob.setApplicationId(appId.toString());
        workflowJob.setInputContext(workflowConfig.getInputProperties());
        workflowJob.setStartTimeInMillis(System.currentTimeMillis());
        workflowEntityMgr.create(workflowJob);

        return appId;
    }

    @Override
    public WorkflowExecutionId getWorkflowId(ApplicationId appId) {
        return workflowJobEntityMgr.findByApplicationId(appId.toString()).getAsWorkflowId();
    }

    private Job createJob(WorkflowConfiguration workflowConfig) {
        Job job = new Job();

        String customer = workflowConfig.getCustomerSpace().toString();
        job.setClient("workflowapiClient");
        job.setCustomer(customer);

        Properties appMasterProperties = new Properties();
        appMasterProperties.put(AppMasterProperty.CUSTOMER.name(), customer);
        appMasterProperties.put(AppMasterProperty.QUEUE.name(), LedpQueueAssigner.getWorkflowQueueNameForSubmission());
        appMasterProperties.put("time", String.valueOf(System.currentTimeMillis()));
        appMasterProperties.put(AppMasterProperty.APP_NAME_SUFFIX.name(),
                workflowConfig.getWorkflowName().replace(" ", "_"));
        appMasterProperties.put(AppMasterProperty.MAX_APP_ATTEMPTS.name(), "1");

        Properties containerProperties = new Properties();
        containerProperties.put(WorkflowProperty.WORKFLOWCONFIG, workflowConfig.toString());
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");

        Integer mem = workflowConfig.getContainerMemoryMB();
        if (mem == null) {
            mem = 2048;
        }
        log.info("Set container memory to " + mem + " mb.");

        appMasterProperties.put(AppMasterProperty.VIRTUALCORES.name(), "1");
        appMasterProperties.put(AppMasterProperty.MEMORY.name(), String.valueOf(mem));
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), "1");
        containerProperties.put(ContainerProperty.MEMORY.name(), String.valueOf(mem));

        String swpkgName = workflowConfig.getSwpkgName();
        if (StringUtils.isNotBlank(swpkgName)) {
            containerProperties.put(ContainerProperty.SWLIB_PKG.name(), swpkgName);
        }

        job.setAppMasterPropertiesObject(appMasterProperties);
        job.setContainerPropertiesObject(containerProperties);

        return job;
    }

    @Override
    public com.latticeengines.domain.exposed.workflow.Job getJobByApplicationId(String applicationId) {
        WorkflowJob workflowJob = workflowJobEntityMgr.findByApplicationId(applicationId);
        if (workflowJob == null) {
            throw new LedpException(LedpCode.LEDP_28023, new String[] { applicationId });
        }

        WorkflowExecutionId workflowId = workflowJob.getAsWorkflowId();
        if (workflowId == null) {
            com.latticeengines.domain.exposed.workflow.Job job = getJobFromWorkflowJobAndYarn(workflowJob);
            return job;
        }
        return workflowService.getJob(workflowId);
    }

    @Override
    public List<com.latticeengines.domain.exposed.workflow.Job> getJobsByTenant(long tenantPid) {
        Tenant tenant = workflowTenantService.getTenantByTenantPid(tenantPid);
        List<WorkflowJob> workflowJobs = workflowJobEntityMgr.findByTenant(tenant);

        List<com.latticeengines.domain.exposed.workflow.Job> jobs = new ArrayList<>();
        List<WorkflowExecutionId> workflowIds = new ArrayList<>();

        for (WorkflowJob workflowJob : workflowJobs) {
            if (workflowJob.getInputContextValue(WorkflowContextConstants.Inputs.JOB_TYPE) != null) {
                WorkflowExecutionId workflowId = workflowJob.getAsWorkflowId();
                if (workflowId == null || (workflowJob.getStatus() != null
                        && workflowJob.getStatus().equals(FinalApplicationStatus.FAILED))) {
                    com.latticeengines.domain.exposed.workflow.Job job = getJobFromWorkflowJobAndYarn(workflowJob);
                    jobs.add(job);
                } else {
                    workflowIds.add(workflowId);
                }
            }
        }

        try {
            jobs.addAll(workflowService.getJobs(workflowIds));
        } catch (Exception e) {
            log.warn(String.format("Error while getting jobs for ids %s, with error %s", workflowIds.toString(),
                    e.getMessage()));
        }

        return jobs;
    }

    @Override
    public com.latticeengines.domain.exposed.workflow.Job getJobFromWorkflowJobAndYarn(WorkflowJob workflowJob) {
        com.latticeengines.domain.exposed.workflow.Job job = new com.latticeengines.domain.exposed.workflow.Job();
        Map<String, String> inputProperties = workflowJob.getInputContext();
        job.setJobType(inputProperties.get(WorkflowContextConstants.Inputs.JOB_TYPE));
        job.setInputs(inputProperties);
        job.setId(workflowJob.getWorkflowId());
        job.setApplicationId(workflowJob.getApplicationId());
        job.setUser(workflowJob.getUserId());
        job.setOutputs(workflowJob.getOutputContext());
        if (workflowJob.getStartTimeInMillis() != null) {
            job.setStartTimestamp(new Date(workflowJob.getStartTimeInMillis()));
        }

        // get state first from database
        if (workflowJob.getStatus() != null && workflowJob.getStatus().equals(FinalApplicationStatus.FAILED)) {
            job.setJobStatus(JobStatus.FAILED);
        } else {
            WorkflowUtils.updateJobFromYarn(job, workflowJob, jobProxy, workflowJobEntityMgr);
        }
        return job;
    }

    @Override
    public List<com.latticeengines.domain.exposed.workflow.Job> getJobsByTenant(long tenantPid, String type) {
        List<com.latticeengines.domain.exposed.workflow.Job> jobs = new ArrayList<>();

        try {
            jobs.addAll(getJobsByTenant(tenantPid));
            if (type != null) {
                Iterator<com.latticeengines.domain.exposed.workflow.Job> iter = jobs.iterator();
                while (iter.hasNext()) {
                    if (!iter.next().getJobType().equals(type)) {
                        iter.remove();
                    }
                }
            }
        } catch (Exception e) {
            log.warn(String.format("Error while getting jobs for tenant pid %s, with error %s", tenantPid,
                    e.getMessage()));
        }

        return jobs;
    }

    @VisibleForTesting
    void setJobProxy(JobProxy jobProxy) {
        this.jobProxy = jobProxy;
    }

}
