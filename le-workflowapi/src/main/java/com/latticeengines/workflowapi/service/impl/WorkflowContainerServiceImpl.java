package com.latticeengines.workflowapi.service.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.batch.BatchService;
import com.latticeengines.aws.batch.JobRequest;
import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.util.JacocoUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.exception.ErrorDetails;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.domain.exposed.workflow.WorkflowProperty;
import com.latticeengines.domain.exposed.workflowapi.WorkflowLogLinks;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;
import com.latticeengines.workflow.exposed.service.JobCacheService;
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

    @Inject
    private JobEntityMgr jobEntityMgr;

    @Inject
    private JobService jobService;

    @Inject
    private JobCacheService jobCacheService;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Inject
    private WorkflowJobUpdateEntityMgr workflowJobUpdateEntityMgr;

    @Inject
    private WorkflowTenantService workflowTenantService;

    @Inject
    private BatchService batchService;

    @Inject
    private JobNameService jobNameService;

    @Inject
    private EMRService emrService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${hadoop.yarn.timeline-service.webapp.address}")
    private String ambariTimelineServiceAddress;

    @Value("${hadoop.yarn.resourcemanager.webapp.address}")
    private String ambariRMAddress;

    @Value("${dataplatform.trustore.jks}")
    private String trustStoreJks;

    @Value("${yarn.jvm.stack.size}")
    private int stackSize;

    @Override
    public ApplicationId submitWorkflow(WorkflowConfiguration workflowConfig, Long workflowPid) {
        WorkflowJob workflowJob = upsertWorkflowJob(workflowConfig, workflowPid);

        if (workflowJob == null) {
            log.error(String.format("Failed to upsert workflowJob. WorkflowPid=%s, WorkflowConfig=%s",
                    workflowPid, JsonUtils.serialize(workflowConfig)));
            return null;
        }
        try {
            Job job = createJob(workflowConfig);
            ApplicationId appId = jobService.submitJob(job);
            job.setId(appId.toString());
            jobEntityMgr.create(job);

            workflowJob.setApplicationId(appId.toString());
            if (Boolean.TRUE.equals(useEmr)) {
                workflowJob.setEmrClusterId(jobService.getEmrClusterId());
                workflowJobEntityMgr.updateApplicationIdAndEmrClusterId(workflowJob);
            } else {
                workflowJobEntityMgr.updateApplicationId(workflowJob);
            }

            jobCacheService.evict(workflowJob.getTenant());

            return appId;
        } catch (Exception exc) {
            workflowJob.setStatus(JobStatus.FAILED.name());
            workflowJobEntityMgr.updateWorkflowJobStatus(workflowJob);
            if (workflowJob.getWorkflowId() != null) {
                jobCacheService.evictByWorkflowIds(Collections.singletonList(workflowJob.getWorkflowId()));
            }

            ErrorDetails details;
            if (exc instanceof LedpException) {
                LedpException casted = (LedpException) exc;
                details = casted.getErrorDetails();
            } else {
                details = new ErrorDetails(LedpCode.LEDP_00002, exc.getMessage(), ExceptionUtils.getStackTrace(exc));
            }
            workflowJob.setErrorDetails(details);
            workflowJobEntityMgr.updateErrorDetails(workflowJob);
            log.warn("Failed to launch a YARN container. Setting status to FAILED for workflowJob, pid=" +
                    workflowJob.getPid() + "\n" + ExceptionUtils.getStackTrace(exc));

            return null;
        }
    }

    @Override
    public String submitAwsWorkflow(WorkflowConfiguration workflowConfig, Long workflowPid) {
        WorkflowJob workflowJob = upsertWorkflowJob(workflowConfig, workflowPid);

        if (workflowJob == null) {
            log.error(String.format("Failed to upsert workflowJob. WorkflowPid=%s, WorkflowConfig=%s",
                    workflowPid, JsonUtils.serialize(workflowConfig)));
            return null;
        }

        try {
            Job job = createJob(workflowConfig);
            JobRequest jobRequest = createJobRequest(workflowConfig);
            String jobId = batchService.submitJob(jobRequest);
            job.setId(jobId);
            jobEntityMgr.create(job);

            workflowJob.setApplicationId(jobId);
            workflowJobEntityMgr.updateApplicationId(workflowJob);

            jobCacheService.evict(workflowJob.getTenant());

            return jobId;
        } catch (Exception exc) {
            workflowJob.setStatus(JobStatus.FAILED.name());
            workflowJobEntityMgr.updateWorkflowJobStatus(workflowJob);
            if (workflowJob.getWorkflowId() != null) {
                jobCacheService.evictByWorkflowIds(Collections.singletonList(workflowJob.getWorkflowId()));
            }

            ErrorDetails details;
            if (exc instanceof LedpException) {
                details = ((LedpException) exc).getErrorDetails();
            } else {
                details = new ErrorDetails(LedpCode.LEDP_00002, exc.getMessage(), ExceptionUtils.getStackTrace(exc));
            }
            workflowJob.setErrorDetails(details);
            workflowJobEntityMgr.updateErrorDetails(workflowJob);
            log.warn("Failed to submit job request to AWS batch. Setting status to FAILED for workflowJob, pid=" +
                    workflowJob.getPid() + "\n" + ExceptionUtils.getStackTrace(exc));

            return null;
        }
    }

    @Override
    public com.latticeengines.domain.exposed.dataplatform.JobStatus getJobStatus(String applicationId, String clusterId) {
        try {
            return jobService.getJobStatusByCluster(applicationId, clusterId);
        } catch (Exception e) {
            if (ExceptionUtils.indexOfThrowable(e, ApplicationNotFoundException.class) > -1) {
                // return null when application does not exist in RM
                return null;
            }
            throw e;
        }
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

    private WorkflowJob upsertWorkflowJob(WorkflowConfiguration workflowConfig, Long workflowPid) {
        if (workflowPid == null) {
            Tenant tenant = workflowTenantService.getTenantFromConfiguration(workflowConfig);
            String user = workflowConfig.getUserId();
            user = user != null ? user : WorkflowUser.DEFAULT_USER.name();

            Long currentTime = System.currentTimeMillis();
            WorkflowJob workflowJob = new WorkflowJob();
            workflowJob.setTenant(tenant);
            workflowJob.setUserId(user);
            workflowJob.setInputContext(workflowConfig.getInputProperties());
            logInputContext(workflowJob);
            workflowJob.setStatus(JobStatus.PENDING.name());
            workflowJob.setStartTimeInMillis(currentTime);
            workflowJob.setType(workflowConfig.getWorkflowName());
            workflowJobEntityMgr.create(workflowJob);

            Long pid = workflowJob.getPid();
            WorkflowJobUpdate jobUpdate = new WorkflowJobUpdate();
            jobUpdate.setWorkflowPid(pid);
            jobUpdate.setCreateTime(currentTime);
            jobUpdate.setLastUpdateTime(currentTime);
            workflowJobUpdateEntityMgr.create(jobUpdate);

            return workflowJob;
        } else {
            WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowPid(workflowPid);

            if (workflowJob == null) {
                log.error("Failed to find workflowJob. WorkflowPid=" + workflowPid);
                return null;
            }

            Tenant tenant = workflowTenantService.getTenantFromConfiguration(workflowConfig);
            String user = workflowConfig.getUserId();
            user = user != null ? user : WorkflowUser.DEFAULT_USER.name();

            workflowJob.setTenant(tenant);
            workflowJob.setUserId(user);
            workflowJob.setInputContext(workflowConfig.getInputProperties());
            logInputContext(workflowJob);
            workflowJob.setType(workflowConfig.getWorkflowName());
            workflowJobEntityMgr.update(workflowJob);
            WorkflowJobUpdate jobUpdate = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowPid);
            jobUpdate.setLastUpdateTime(System.currentTimeMillis());
            workflowJobUpdateEntityMgr.updateLastUpdateTime(jobUpdate);

            return workflowJob;
        }
    }

    private void logInputContext(WorkflowJob workflowJob) {
        String inputContextString = workflowJob.getInputContextString();
        if (inputContextString != null && inputContextString.length() > 3500) {
            log.warn("Workflow job's input context is too large, pid=" + workflowJob.getPid() + ", input context="
                    + inputContextString);
        }
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
        appMasterProperties.put(AppMasterProperty.JVM_STACK.name(), String.valueOf(stackSize));
        Properties containerProperties = new Properties();
        containerProperties.put(WorkflowProperty.WORKFLOWCONFIG, workflowConfig.toString());
        containerProperties.put(ContainerProperty.PRIORITY.name(), "0");

        Integer mem = workflowConfig.getContainerMemoryMB();
        if (mem == null) {
            mem = 2048;
        }
        int cores = Math.max(1, mem / 1024 / 4);
        log.info("Set container memory to " + mem + " mb, cpu to " + cores + " cores.");
        appMasterProperties.put(AppMasterProperty.VIRTUALCORES.name(), String.valueOf(cores));
        appMasterProperties.put(AppMasterProperty.MEMORY.name(), String.valueOf(mem));
        containerProperties.put(ContainerProperty.VIRTUALCORES.name(), String.valueOf(cores));
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

    @Override
    public WorkflowLogLinks getLogUrlByWorkflowPid(Long workflowPid) {
        if (workflowPid == null) {
            throw new IllegalArgumentException("Cannot use null workflow pid");
        }

        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowPid(workflowPid);
        if (workflowJob == null) {
            throw new IllegalArgumentException("Cannot find a workflow with pid " + workflowPid);
        }

        String appId = workflowJob.getApplicationId();
        if (StringUtils.isNotBlank(appId)) {
            WorkflowLogLinks logLinks = new WorkflowLogLinks();
            String amUrl = null;
            String emrClusterId = workflowJob.getEmrClusterId();
            if (StringUtils.isNotBlank(emrClusterId)) {
                boolean clusterIsActvie = emrService.isActive(emrClusterId);
                if (clusterIsActvie) {
                    // if active, go to job history page or rm page
                    String masterIp = emrService.getMasterIp(emrClusterId);
                    JobStatus status = JobStatus.fromString(workflowJob.getStatus());
                    amUrl = status.isTerminated() ? appHistoryUrl(masterIp, appId) : appMasterUrl(masterIp, appId);
                }
                // for all emr, try to give s3 address
                String bucket = emrService.getLogBucket(emrClusterId);
                if (StringUtils.isNotBlank(bucket)) {
                    String s3LogDir = s3LogUrl(bucket, emrClusterId, appId);
                    logLinks.setS3LogDir(s3LogDir);
                } else {
                    log.warn("Cannot find log bucket for emr cluster " + emrClusterId);
                }
            } else {
                JobStatus status = JobStatus.fromString(workflowJob.getStatus());
                amUrl = status.isTerminated() ? ambariAppHistoryUrl(appId) : ambariAppMasterUrl(appId);
            }
            logLinks.setAppMasterUrl(amUrl);
            return logLinks;
        } else {
            throw new IllegalStateException("Workflow with pid " + workflowPid + " does not have an app id, " //
                    + "therefore cannot find the log link.");
        }
    }

    @Override
    public String getCurrentClusterId() {
        return jobService.getEmrClusterId();
    }

    private String ambariAppHistoryUrl(String appId) {
        return String.format("%s/app/%s", ambariTimelineServiceAddress, appId);
    }

    private String ambariAppMasterUrl(String appId) {
        return String.format("http://%s/cluster/app/%s", ambariRMAddress, appId);
    }

    private String appHistoryUrl(String masterIp, String appId) {
        return String.format("http://%s:8188/applicationhistory/app/%s", masterIp, appId);
    }

    private String appMasterUrl(String masterIp, String appId) {
        return String.format("http://%s:8088/cluster/app/%s", masterIp, appId);
    }

    private String s3LogUrl(String bucket, String clusterId, String appId) {
        return "https://s3.console.aws.amazon.com/s3/buckets/" //
                + bucket + "/" + clusterId + "/containers/" + appId + "/";
    }

}
