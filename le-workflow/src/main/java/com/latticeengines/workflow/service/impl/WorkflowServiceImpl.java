package com.latticeengines.workflow.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.TransformerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.JobFactory;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.ReferenceJobFactory;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.launch.NoSuchJobInstanceException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.repository.dao.JobExecutionDao;
import org.springframework.batch.core.repository.dao.StepExecutionDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowInstanceId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowJobUpdate;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.workflow.core.LEJobExecutionRetriever;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobUpdateEntityMgr;
import com.latticeengines.workflow.exposed.service.JobCacheService;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.exposed.service.WorkflowTenantService;
import com.latticeengines.workflow.exposed.user.WorkflowUser;
import com.latticeengines.workflow.exposed.util.WorkflowUtils;
import com.latticeengines.workflow.listener.LEJobCaller;
import com.latticeengines.workflow.listener.LEJobCallerRegister;

@Component("workflowService")
public class WorkflowServiceImpl implements WorkflowService {

    private static final Logger log = LoggerFactory.getLogger(WorkflowServiceImpl.class);

    @Value("${hadoop.yarn.timeline-service.webapp.address}")
    private String timelineServiceUrl;

    private static final String WORKFLOW_SERVICE_UUID = "WorkflowServiceUUID";
    private static final String CUSTOMER_SPACE = "CustomerSpace";
    private static final String INTERNAL_RESOURCE_HOST_PORT = "Internal_Resource_Host_Port";
    private static final String USER_ID = "User_Id";
    private static final long MAX_MILLIS_TO_WAIT = 1000L * 60 * 60 * 36;
    public static final long HEARTBEAT_MILLIS = 1000 * 120;

    @Autowired
    private LEJobExecutionRetriever leJobExecutionRetriever;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRegistry jobRegistry;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private WorkflowTenantService workflowTenantService;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Autowired
    private JobCacheService jobCacheService;

    @Autowired
    private WorkflowJobUpdateEntityMgr workflowJobUpdateEntityMgr;

    @Autowired
    private LEJobCallerRegister callerRegister;

    @Override
    public <T extends WorkflowConfiguration> void registerJob(T workflowConfig, ApplicationContext context) {
        try {
            @SuppressWarnings("unchecked")
            AbstractWorkflow<T> workflow = context.getBean(workflowConfig.getWorkflowName(), AbstractWorkflow.class);
            Job job = workflow.buildWorkflow(workflowConfig);
            JobFactory jobFactory = new ReferenceJobFactory(job);
            jobRegistry.register(jobFactory);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void unRegisterJob(String workflowName) {
        jobRegistry.unregister(workflowName);
    }

    @Override
    public long startWorkflowJob(WorkflowConfiguration workflowConfiguration) {
        Job workflow = null;
        try {
            workflow = jobRegistry.getJob(workflowConfiguration.getWorkflowName());
        } catch (NoSuchJobException e1) {
            throw new LedpException(LedpCode.LEDP_28000, new String[] { workflowConfiguration.getWorkflowName() });
        }

        JobParameters parms = createJobParams(workflowConfiguration);
        JobExecution jobExecution = null;
        try {
            jobExecution = jobLauncher.run(workflow, parms);
        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
                | JobParametersInvalidException e) {
            throw new LedpException(LedpCode.LEDP_28001, e, new String[] { workflowConfiguration.getWorkflowName() });
        }
        return jobExecution.getId();
    }

    @Override
    public JobParameters createJobParams(WorkflowConfiguration workflowConfiguration) {
        JobParametersBuilder parmsBuilder = new JobParametersBuilder().addString(WORKFLOW_SERVICE_UUID,
                UUID.randomUUID().toString());
        if (workflowConfiguration != null) {
            if (workflowConfiguration.getCustomerSpace() != null) {
                parmsBuilder.addString(CUSTOMER_SPACE, workflowConfiguration.getCustomerSpace().toString());
            }
            if (workflowConfiguration.getInternalResourceHostPort() != null) {
                parmsBuilder.addString(INTERNAL_RESOURCE_HOST_PORT,
                        workflowConfiguration.getInternalResourceHostPort());
            }
            if (workflowConfiguration.getUserId() != null) {
                parmsBuilder.addString(USER_ID, workflowConfiguration.getUserId());
            } else {
                parmsBuilder.addString(USER_ID, WorkflowUser.DEFAULT_USER.name());
            }
            Map<String, String> flatteredConfig = WorkflowUtils.getFlattenedConfig(workflowConfiguration);
            flatteredConfig.forEach(parmsBuilder::addString);
        }

        return parmsBuilder.toJobParameters();
    }

    @Override
    public WorkflowExecutionId start(WorkflowConfiguration workflowConfiguration) {
        long jobExecutionId = startWorkflowJob(workflowConfiguration);
        Tenant tenant = workflowTenantService.getTenantFromConfiguration(workflowConfiguration);
        String user = workflowConfiguration.getUserId();
        user = user != null ? user : WorkflowUser.DEFAULT_USER.name();

        Long currentTime = System.currentTimeMillis();
        String workflowType = workflowConfiguration.getWorkflowName();
        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setTenant(tenant);
        workflowJob.setUserId(user);
        workflowJob.setWorkflowId(jobExecutionId);
        workflowJob.setType(workflowType);
        workflowJob.setStatus(JobStatus.RUNNING.name());
        workflowJob.setStartTimeInMillis(currentTime);
        workflowJobEntityMgr.create(workflowJob);

        Long workflowPid = workflowJobEntityMgr.findByWorkflowId(workflowJob.getWorkflowId()).getPid();
        WorkflowJobUpdate jobUpdate = new WorkflowJobUpdate();
        jobUpdate.setWorkflowPid(workflowPid);
        jobUpdate.setCreateTime(currentTime);
        jobUpdate.setLastUpdateTime(currentTime);
        workflowJobUpdateEntityMgr.create(jobUpdate);

        jobCacheService.evict(tenant);

        return new WorkflowExecutionId(jobExecutionId);
    }

    @Override
    public WorkflowExecutionId start(WorkflowConfiguration workflowConfiguration, WorkflowJob workflowJob) {
        long jobExecutionId = startWorkflowJob(workflowConfiguration);
        workflowJob.setWorkflowId(jobExecutionId);
        workflowJob.setType(workflowConfiguration.getWorkflowName());
        workflowJob.setStatus(JobStatus.RUNNING.name());
        workflowJobEntityMgr.registerWorkflowId(workflowJob);
        workflowJobEntityMgr.updateWorkflowJobStatus(workflowJob);

        Long currentTime = System.currentTimeMillis();
        WorkflowJobUpdate jobUpdate = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowJob.getPid());
        jobUpdate.setCreateTime(currentTime);
        jobUpdate.setLastUpdateTime(currentTime);
        workflowJobUpdateEntityMgr.updateLastUpdateTime(jobUpdate);

        jobCacheService.evict(workflowJob.getTenant());

        log.info(String.format("Start workflow with jobExecutionId=%d", jobExecutionId));
        return new WorkflowExecutionId(jobExecutionId);
    }

    @Override
    public WorkflowExecutionId restart(WorkflowInstanceId workflowInstanceId, WorkflowJob workflowJob) {
        Long jobExecutionId = -1L;
        List<Long> jobExecutions;
        try {
            jobExecutions = jobOperator.getExecutions(workflowInstanceId.getId());
        } catch (NoSuchJobInstanceException e) {
            throw new LedpException(LedpCode.LEDP_28002, e, new String[] { String.valueOf(workflowInstanceId.getId()),
                    String.valueOf(workflowInstanceId.getId()) });
        }

        jobExecutionId = jobExecutions.get(0);
        log.info(String.format("Restarting workflowId:%d from most recent jobExecutionId:%d.",
                workflowInstanceId.getId(), jobExecutionId));
        return restart(new WorkflowExecutionId(jobExecutionId), workflowJob);
    }

    @Override
    public WorkflowExecutionId restart(WorkflowExecutionId workflowExecutionId, WorkflowJob workflowJob) {
        Long jobExecutionId = 0L;

        try {
            JobExecutionDao jobExecutionDao = leJobExecutionRetriever.getJobExecutionDao();
            StepExecutionDao stepExecutionDao = leJobExecutionRetriever.getStepExecutionDao();
            JobExecution jobExecution = leJobExecutionRetriever.getJobExecution(
                    workflowExecutionId.getId(), Boolean.TRUE);
            BatchStatus jobStatus = jobExecution.getStatus();
            if (jobStatus.isRunning() || jobStatus == BatchStatus.STOPPING) {
                log.info(String.format("Spring-batch status=%s, will set it to failed.", jobStatus.name()));
                for (StepExecution stepExecution : jobExecution.getStepExecutions()) {
                    if (stepExecution.getStatus().isRunning()) {
                        stepExecution.setStatus(BatchStatus.STOPPED);
                        stepExecution.setEndTime(new Date());
                        stepExecutionDao.updateStepExecution(stepExecution);
                    }
                }
                jobExecution.setStatus(BatchStatus.FAILED);
                jobExecution.setEndTime(new Date());
                jobExecutionDao.updateJobExecution(jobExecution);
            }

            jobExecutionId = jobOperator.restart(workflowExecutionId.getId());
            workflowJob.setWorkflowId(jobExecutionId);
            workflowJob.setStatus(JobStatus.RUNNING.name());
            workflowJobEntityMgr.registerWorkflowId(workflowJob);
            workflowJobEntityMgr.updateWorkflowJobStatus(workflowJob);

            WorkflowJobUpdate jobUpdate = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowJob.getPid());
            if (jobUpdate == null) {
                Long currentTime = System.currentTimeMillis();
                jobUpdate = new WorkflowJobUpdate();
                jobUpdate.setWorkflowPid(workflowJob.getPid());
                jobUpdate.setCreateTime(currentTime);
                jobUpdate.setLastUpdateTime(currentTime);
                workflowJobUpdateEntityMgr.create(jobUpdate);
            } else {
                jobUpdate.setLastUpdateTime(System.currentTimeMillis());
                workflowJobUpdateEntityMgr.updateLastUpdateTime(jobUpdate);
            }

            jobCacheService.evict(workflowJob.getTenant());

            log.info(String.format("Restart workflow from jobExecutionId=%d. Created new jobExecutionId=%d",
                    workflowExecutionId.getId(), jobExecutionId));
        } catch (JobInstanceAlreadyCompleteException | NoSuchJobExecutionException | NoSuchJobException
                | JobRestartException | JobParametersInvalidException e) {
            throw new LedpException(LedpCode.LEDP_28002, e, new String[] { String.valueOf(workflowExecutionId.getId()),
                    String.valueOf(workflowExecutionId.getId()) });
        }

        return new WorkflowExecutionId(jobExecutionId);
    }

    @Override
    public void stop(WorkflowExecutionId workflowId) {
        try {
            WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(workflowId.getId());
            if (job != null) {
                jobOperator.stop(workflowId.getId());
            } else {
                throw new LedpException(LedpCode.LEDP_28000, new String[] { String.valueOf(workflowId.getId()) });
            }
        } catch (NoSuchJobExecutionException | JobExecutionNotRunningException | IllegalArgumentException e) {
            throw new LedpException(LedpCode.LEDP_28003, e, new String[] { String.valueOf(workflowId.getId()) });
        }
    }

    @Override
    public WorkflowStatus getStatus(WorkflowExecutionId workflowId) {
        JobExecution jobExecution = leJobExecutionRetriever.getJobExecution(workflowId.getId());
        return getStatus(workflowId, jobExecution);
    }

    @Override
    public JobStatus getJobStatus(WorkflowExecutionId workflowId) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(workflowId.getId());
        return JobStatus.valueOf(job.getStatus());
    }

    @Override
    public WorkflowStatus getStatus(WorkflowExecutionId workflowId, JobExecution jobExecution) {
        WorkflowStatus workflowStatus = new WorkflowStatus();
        workflowStatus.setStatus(jobExecution.getStatus());
        workflowStatus.setStartTime(jobExecution.getStartTime());
        workflowStatus.setEndTime(jobExecution.getEndTime());
        workflowStatus.setLastUpdated(jobExecution.getLastUpdated());
        workflowStatus.setWorkflowName(getWorkflowName(workflowId));

        String customerSpace = jobExecution.getJobParameters().getString(CUSTOMER_SPACE);
        if (!Strings.isNullOrEmpty(customerSpace)) {
            workflowStatus.setCustomerSpace(CustomerSpace.parse(customerSpace));
        }
        return workflowStatus;
    }

    private String getWorkflowName(WorkflowExecutionId workflowId) {
        return getWorkflowName(leJobExecutionRetriever.getJobExecution(workflowId.getId()));
    }

    private String getWorkflowName(JobExecution jobExecution) {
        return jobExecution.getJobInstance().getJobName();
    }

    @Override
    public List<String> getStepNames(WorkflowExecutionId workflowId) {
        JobExecution jobExecution = leJobExecutionRetriever.getJobExecution(workflowId.getId());

        return getStepNamesFromExecution(jobExecution);
    }

    private List<String> getStepNamesFromExecution(JobExecution jobExecution) {
        Collection<String> stepNames = CollectionUtils.collect(jobExecution.getStepExecutions(),
                TransformerUtils.invokerTransformer("getStepName"));

        return new ArrayList<>(stepNames);
    }

    @Override
    public WorkflowStatus waitForCompletion(WorkflowExecutionId workflowId) throws Exception {
        return waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT);
    }

    @Override
    public WorkflowStatus waitForCompletion(WorkflowExecutionId workflowId, long maxWaitTime) throws Exception {
        return waitForCompletion(workflowId, maxWaitTime, HEARTBEAT_MILLIS);
    }

    @Override
    public WorkflowStatus waitForCompletion(WorkflowExecutionId workflowId, long maxWaitTime, long checkInterval)
            throws Exception {
        WorkflowStatus status = null;
        long start = System.currentTimeMillis();
        int retryOnException = 16;

        WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(workflowId.getId());
        if (workflowJob == null) {
            return null;
        }

        Long workflowPid = workflowJob.getPid();
        WorkflowJobUpdate jobUpdate = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowPid);
        // break label for inner loop
        do {
            try {
                status = getStatus(workflowId);
                keepUpdate(workflowPid, jobUpdate);
                if (status == null) {
                    break;
                } else if (WorkflowStatus.TERMINAL_BATCH_STATUS.contains(status.getStatus())) {
                    workflowJob.setStatus(JobStatus.fromString(status.getStatus().name()).name());
                    workflowJobEntityMgr.updateWorkflowJobStatus(workflowJob);
                    if (workflowJob.getWorkflowId() != null) {
                        jobCacheService.evictByWorkflowIds(Collections.singletonList(workflowJob.getWorkflowId()));
                    }
                    break;
                }
            } catch (Exception e) {
                log.warn(String.format("Error while getting status for workflow %d, with error %s", workflowId.getId(),
                        e.getMessage()));
                if (--retryOnException == 0)
                    throw e;
            } finally {
                Thread.sleep(checkInterval);
            }
        } while (System.currentTimeMillis() - start < maxWaitTime);

        return status;
    }

    private void keepUpdate(Long workflowPid, WorkflowJobUpdate jobUpdate) {
        if (jobUpdate == null)
            return;
        long currentTime = System.currentTimeMillis();
        jobUpdate.setLastUpdateTime(currentTime);
        workflowJobUpdateEntityMgr.updateLastUpdateTime(jobUpdate);
    }

    @Override
    public Map<String, String> getInputs(Map<String, String> inputContext) {
        Map<String, String> inputs = new HashMap<>();
        for (Map.Entry<String, String> entry : inputContext.entrySet()) {
            inputs.put(entry.getKey(), entry.getValue());
        }
        return inputs;
    }

    @Override
    public JobStatus sleepForCompletionWithStatus(WorkflowExecutionId workflowId) {
        sleepForCompletion(workflowId);
        return getJobStatus(workflowId);
    }

    @Override
    public void sleepForCompletion(WorkflowExecutionId workflowId) {
        long maxWaitTime = MAX_MILLIS_TO_WAIT;
        long checkInterval = 1000 * 120;
        JobWaitCaller caller = new JobWaitCaller(workflowId, maxWaitTime, checkInterval);
        synchronized (callerRegister) {
            callerRegister.register(Thread.currentThread(), caller);
            callerRegister.notifyAll();
        }
        caller.sleep();
        log.info(String.format("Finished waiting for the workflow id= %s", workflowId.getId()));
    }

    private class JobWaitCaller implements LEJobCaller {
        // report once every X wait periods after the job has been running longer than
        // maxWaitTime
        private static final int LONG_RUNNING_JOB_REPORT_PERIOD = 30;

        private long checkInterval;
        private long maxWaitTime;
        private WorkflowExecutionId workflowId;
        private volatile boolean isDone = false;

        public JobWaitCaller(WorkflowExecutionId workflowId, long maxWaitTime, long checkInterval) {
            this.maxWaitTime = maxWaitTime;
            this.checkInterval = checkInterval;
            this.workflowId = workflowId;
        }

        public boolean sleep() {
            long start = System.currentTimeMillis();
            int nWaitPeriodAfterMaxWaitTime = 0;
            int retryOnException = 16;
            WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(workflowId.getId());
            long workflowPid = workflowJob.getPid();
            WorkflowJobUpdate jobUpdate = workflowJobUpdateEntityMgr.findByWorkflowPid(workflowPid);
            while (true) {
                if (isDone) {
                    return true;
                }
                try {
                    keepUpdate(workflowPid, jobUpdate);
                    if (workflowId != null) {
                        // pre-warm the cache
                        jobCacheService.putAsync(workflowId.getId());
                    }
                    Thread.sleep(checkInterval);
                } catch (Exception e) {
                    if (isDone) {
                        keepUpdate(workflowPid, jobUpdate);
                        return true;
                    }
                    log.warn(
                            String.format("Getting exception when waiting for %s, exception=%s",
                                    workflowId.getId(), e.getMessage()));
                    if (--retryOnException == 0) {
                        throw new RuntimeException(e);
                    }
                }
                log.info("Waiting workflow to finish. workflow Id=" + workflowId.getId());
                long currentRunningTime = System.currentTimeMillis() - start;
                if (currentRunningTime >= maxWaitTime) {
                    if (nWaitPeriodAfterMaxWaitTime % LONG_RUNNING_JOB_REPORT_PERIOD == 0) {
                        log.error("Job (PID={}) has been running for {} hours", workflowPid,
                                TimeUnit.MILLISECONDS.toHours(currentRunningTime));
                    }
                    nWaitPeriodAfterMaxWaitTime++;
                }
            }
        }

        @Override
        public void callDone() {
            isDone = true;
        }
    }

    @VisibleForTesting
    public void setLeJobExecutionRetriever(LEJobExecutionRetriever leJobExecutionRetriever) {
        this.leJobExecutionRetriever = leJobExecutionRetriever;
    }
}
