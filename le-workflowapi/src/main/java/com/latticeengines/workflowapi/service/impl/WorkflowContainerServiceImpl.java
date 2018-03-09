package com.latticeengines.workflowapi.service.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.batch.BatchService;
import com.latticeengines.aws.batch.JobRequest;
import com.latticeengines.common.exposed.util.JacocoUtils;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.domain.exposed.workflow.WorkflowProperty;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowTenantService;
import com.latticeengines.workflow.exposed.user.WorkflowUser;
import com.latticeengines.workflowapi.service.WorkflowContainerService;
import com.latticeengines.yarn.exposed.client.AppMasterProperty;
import com.latticeengines.yarn.exposed.client.ContainerProperty;
import com.latticeengines.yarn.exposed.entitymanager.JobEntityMgr;
import com.latticeengines.yarn.exposed.service.JobNameService;
import com.latticeengines.yarn.exposed.service.JobService;
import com.latticeengines.yarn.exposed.service.impl.JobNameServiceImpl;

@Component("workflowContainerService")
public class WorkflowContainerServiceImpl implements WorkflowContainerService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowContainerService.class);

    @Autowired
    private JobEntityMgr jobEntityMgr;

    @Autowired
    private JobService jobService;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private WorkflowJobUpdateEntityMgr workflowJobUpdateEntityMgr;

    @Autowired
    private WorkflowTenantService workflowTenantService;

    @Autowired
    private BatchService batchService;

    @Autowired
    private JobNameService jobNameService;

    @Value("${dataplatform.trustore.jks}")
    private String trustStoreJks;

    @Override
    public ApplicationId submitWorkFlow(WorkflowConfiguration workflowConfig) {
        Job job = createJob(workflowConfig);
        ApplicationId appId = jobService.submitJob(job);
        job.setId(appId.toString());

        createWorkflowJob(workflowConfig, job, appId.toString());
        return appId;
    }

    @Override
    public String submitAwsWorkFlow(WorkflowConfiguration workflowConfig) {
        Job job = createJob(workflowConfig);
        JobRequest jobRequest = createJobRequest(workflowConfig);
        String jobId = batchService.submitJob(jobRequest);
        job.setId(jobId);

        createWorkflowJob(workflowConfig, job, jobId);
        return jobId;
    }

    private JobRequest createJobRequest(WorkflowConfiguration workflowConfig) {
        String customer = workflowConfig.getCustomerSpace().toString();
        JobRequest jobRequest = new JobRequest();
        String jobName = jobNameService.createJobName(customer, "Aws") + JobNameServiceImpl.JOBNAME_DELIMITER
                + workflowConfig.getWorkflowName().replace(" ", "_");
        jobName = jobName.replaceAll(JobNameServiceImpl.JOBNAME_DELIMITER + "", "_");
        jobRequest.setJobName(jobName);
        jobRequest.setJobDefinition("AWS-Workflow-Job-Definition");
        jobRequest.setJobQueue("AWS-Workflow-Job-Queue");
        Integer memory = workflowConfig.getContainerMemoryMB();
        if (memory == null) {
            memory = 2048;
        }
        log.info("Set container memory to " + memory + " mb.");
        jobRequest.setMemory(memory);
        jobRequest.setCpus(1);
        Map<String, String> envs = new HashMap<>();
        envs.put(WorkflowProperty.WORKFLOWCONFIG, workflowConfig.toString());
        envs.put(WorkflowProperty.WORKFLOWCONFIGCLASS, workflowConfig.getClass().getName());

        jobRequest.setEnvs(envs);
        return jobRequest;
    }

    private void createWorkflowJob(WorkflowConfiguration workflowConfig, Job job, String appId) {
        jobEntityMgr.create(job);

        Tenant tenant = workflowTenantService.getTenantFromConfiguration(workflowConfig);
        String user = workflowConfig.getUserId();
        user = user != null ? user : WorkflowUser.DEFAULT_USER.name();

        Long currentTime = System.currentTimeMillis();
        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setTenant(tenant);
        workflowJob.setUserId(user);
        workflowJob.setApplicationId(appId);
        workflowJob.setInputContext(workflowConfig.getInputProperties());
        workflowJob.setStatus(JobStatus.PENDING.name());
        workflowJob.setStartTimeInMillis(currentTime);
        workflowJob.setType(workflowConfig.getWorkflowName());
        workflowJobEntityMgr.create(workflowJob);

        Long workflowPid = workflowJob.getPid();
        WorkflowJobUpdate jobUpdate = new WorkflowJobUpdate();
        jobUpdate.setWorkflowPid(workflowPid);
        jobUpdate.setLastUpdateTime(currentTime);
        workflowJobUpdateEntityMgr.create(jobUpdate);
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

        Collection<String> swpkgNames = workflowConfig.getSwpkgNames();
        if (CollectionUtils.isNotEmpty(swpkgNames)) {
            containerProperties.put(ContainerProperty.SWLIB_PKG.name(), String.join(",", swpkgNames));
        }

        JacocoUtils.setJacoco(containerProperties, "workflowapi");

        if (StringUtils.isNotBlank(trustStoreJks)) {
            containerProperties.put(ContainerProperty.TRUST_STORE.name(), trustStoreJks);
        }

        job.setAppMasterPropertiesObject(appMasterProperties);
        job.setContainerPropertiesObject(containerProperties);

        return job;
    }

}
