package com.latticeengines.workflow.service.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.TransformerUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import com.google.common.base.Strings;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowInstanceId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.workflow.core.LEJobExecutionRetriever;
import com.latticeengines.workflow.core.WorkflowExecutionCache;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflow.exposed.service.WorkflowTenantService;
import com.latticeengines.workflow.exposed.user.WorkflowUser;

@Component("workflowService")
public class WorkflowServiceImpl implements WorkflowService {
    private static final Logger log = LoggerFactory.getLogger(WorkflowServiceImpl.class);

    @Value("${hadoop.yarn.timeline-service.webapp.address}")
    private String timelineServiceUrl;

    private static final String WORKFLOW_SERVICE_UUID = "WorkflowServiceUUID";
    private static final String CUSTOMER_SPACE = "CustomerSpace";
    private static final String INTERNAL_RESOURCE_HOST_PORT = "Internal_Resource_Host_Port";
    private static final String USER_ID = "User_Id";
    private static final long MAX_MILLIS_TO_WAIT = 1000L * 60 * 60 * 24;

    @Autowired
    private LEJobExecutionRetriever leJobExecutionRetriever;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobRegistry jobRegistry;

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private WorkflowExecutionCache workflowExecutionCache;

    @Autowired
    private WorkflowTenantService workflowTenantService;

    @Autowired
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Override
    public List<String> getNames() {
        return new ArrayList<String>(jobRegistry.getJobNames());
    }

    @Override
    public void registerJob(String name, ApplicationContext context) {
        Job job;
        try {
            job = context.getBean(name, AbstractWorkflow.class).buildWorkflow();
            JobFactory jobFactory = new ReferenceJobFactory(job);
            jobRegistry.register(jobFactory);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

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
                        workflowConfiguration.getInternalResourceHostPort().toString());
            }
            if (workflowConfiguration.getUserId() != null) {
                parmsBuilder.addString(USER_ID, workflowConfiguration.getUserId());
            } else {
                parmsBuilder.addString(USER_ID, WorkflowUser.DEFAULT_USER.name());
            }
            for (String configurationClassName : workflowConfiguration.getConfigRegistry().keySet()) {
                parmsBuilder.addString(configurationClassName,
                        workflowConfiguration.getConfigRegistry().get(configurationClassName));
            }
        }

        JobParameters parms = parmsBuilder.toJobParameters();
        return parms;
    }

    @Override
    public WorkflowExecutionId start(WorkflowConfiguration workflowConfiguration) {
        long jobExecutionId = startWorkflowJob(workflowConfiguration);
        Tenant tenant = workflowTenantService.getTenantFromConfiguration(workflowConfiguration);
        String user = workflowConfiguration.getUserId();
        user = user != null ? user : WorkflowUser.DEFAULT_USER.name();

        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setTenant(tenant);
        workflowJob.setUserId(user);
        workflowJob.setWorkflowId(jobExecutionId);
        workflowJobEntityMgr.create(workflowJob);

        return new WorkflowExecutionId(jobExecutionId);
    }

    @Override
    public WorkflowExecutionId start(WorkflowConfiguration workflowConfiguration, WorkflowJob workflowJob) {
        long jobExecutionId = startWorkflowJob(workflowConfiguration);
        workflowJob.setWorkflowId(jobExecutionId);
        workflowJobEntityMgr.registerWorkflowId(workflowJob);
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
            jobExecutionId = jobOperator.restart(workflowExecutionId.getId());
            workflowJob.setWorkflowId(jobExecutionId);
            workflowJobEntityMgr.registerWorkflowId(workflowJob);
            log.info(String.format("Restarted workflow from jobExecutionId:%d. Created new jobExecutionId:%d",
                    workflowExecutionId.getId(), jobExecutionId));
        } catch (JobInstanceAlreadyCompleteException | NoSuchJobExecutionException | NoSuchJobException
                | JobRestartException | JobParametersInvalidException e) {
            throw new LedpException(LedpCode.LEDP_28002, e, new String[] { String.valueOf(workflowExecutionId.getId()),
                    String.valueOf(workflowExecutionId.getId()) });
        }

        return new WorkflowExecutionId(jobExecutionId);
    }

    @Override
    public WorkflowExecutionId relaunch(WorkflowConfiguration workflowConfiguration, WorkflowJob workflowJob) {
        JobExecution jobExecution = null;

        try {
            Job workflow = jobRegistry.getJob(workflowConfiguration.getWorkflowName());
            JobParameters parameters = leJobExecutionRetriever
                    .getJobExecution(workflowConfiguration.getWorkflowIdToRestart().getId()).getJobParameters();
            jobExecution = jobLauncher.run(workflow, parameters);

            workflowJob.setWorkflowId(jobExecution.getId());
            workflowJobEntityMgr.registerWorkflowId(workflowJob);
            log.info(String.format("Restarted workflow from jobExecutionId:%d. Created new jobExecutionId:%d",
                    workflowConfiguration.getWorkflowIdToRestart(), jobExecution.getId()));
        } catch (JobInstanceAlreadyCompleteException | NoSuchJobException | JobRestartException
                | JobParametersInvalidException | JobExecutionAlreadyRunningException e) {
            throw new LedpException(LedpCode.LEDP_28002, e,
                    new String[] { String.valueOf(workflowConfiguration.getWorkflowIdToRestart()),
                            String.valueOf(workflowConfiguration.getWorkflowIdToRestart()) });
        }

        return new WorkflowExecutionId(jobExecution.getId());
    }

    @Override
    public void stop(WorkflowExecutionId workflowId) {
        try {
            WorkflowJob workflowJob = workflowJobEntityMgr.findByWorkflowId(workflowId.getId());
            jobOperator.stop(workflowId.getId());
            workflowJob.setStatus(FinalApplicationStatus.KILLED);
            workflowJobEntityMgr.updateWorkflowJob(workflowJob);
        } catch (NoSuchJobExecutionException | JobExecutionNotRunningException e) {
            throw new LedpException(LedpCode.LEDP_28003, e, new String[] { String.valueOf(workflowId.getId()) });
        }
    }

    @Override
    public WorkflowStatus getStatus(WorkflowExecutionId workflowId) {
        JobExecution jobExecution = leJobExecutionRetriever.getJobExecution(workflowId.getId());
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

    @Override
    public com.latticeengines.domain.exposed.workflow.Job getJob(WorkflowExecutionId workflowId) {
        com.latticeengines.domain.exposed.workflow.Job job = workflowExecutionCache.getJob(workflowId);
        if (job.getOutputs() != null && job.getApplicationId() != null) {
            job.getOutputs().put(WorkflowContextConstants.Outputs.YARN_LOG_LINK_PATH,
                    String.format("%s/app/%s", timelineServiceUrl, job.getApplicationId()));
        }
        return job;
    }

    @Override
    public List<com.latticeengines.domain.exposed.workflow.Job> getJobs(List<WorkflowExecutionId> workflowIds) {
        List<com.latticeengines.domain.exposed.workflow.Job> jobs = new ArrayList<>();

        try {
            jobs.addAll(workflowExecutionCache.getJobs(workflowIds));
        } catch (Exception e) {
            log.warn(String.format("Error while getting jobs for ids %s, with error %s", workflowIds.toString(),
                    e.getMessage()));
        }

        return jobs;
    }

    @Override
    public List<com.latticeengines.domain.exposed.workflow.Job> getJobs(List<WorkflowExecutionId> workflowIds,
            String type) {
        List<com.latticeengines.domain.exposed.workflow.Job> jobs = new ArrayList<>();

        try {
            jobs.addAll(workflowExecutionCache.getJobs(workflowIds));
            if (type != null) {
                Iterator<com.latticeengines.domain.exposed.workflow.Job> iter = jobs.iterator();
                while (iter.hasNext()) {
                    if (!iter.next().getJobType().equals(type)) {
                        iter.remove();
                    }
                }
            }
        } catch (Exception e) {
            log.warn(String.format("Error while getting jobs for ids %s, with error %s", workflowIds.toString(),
                    e.getMessage()));
        }

        return jobs;
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
        @SuppressWarnings("unchecked")
        Collection<String> stepNames = CollectionUtils.collect(jobExecution.getStepExecutions(),
                TransformerUtils.invokerTransformer("getStepName"));

        return new ArrayList<String>(stepNames);
    }

    @Override
    public WorkflowStatus waitForCompletion(WorkflowExecutionId workflowId) throws Exception {
        return waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT);
    }

    @Override
    public WorkflowStatus waitForCompletion(WorkflowExecutionId workflowId, long maxWaitTime) throws Exception {
        WorkflowStatus status = null;
        long start = System.currentTimeMillis();
        int retryOnException = 16;

        // break label for inner loop
        done: do {
            try {
                status = getStatus(workflowId);
                if (status == null) {
                    break;
                } else if (WorkflowStatus.TERMINAL_BATCH_STATUS.contains(status.getStatus())) {
                    break done;
                }
            } catch (Exception e) {
                log.warn(String.format("Error while getting status for workflow %d, with error %s", workflowId.getId(),
                        e.getMessage()));
                if (--retryOnException == 0)
                    throw e;
            } finally {
                Thread.sleep(10000);
            }
        } while (System.currentTimeMillis() - start < maxWaitTime);

        return status;
    }

    @Override
    public Map<String, String> getInputs(Map<String, String> inputContext) {
        Map<String, String> inputs = new HashMap<>();
        for (Map.Entry<String, String> entry : inputContext.entrySet()) {
            inputs.put(entry.getKey(), entry.getValue());
        }
        return inputs;
    }
}
