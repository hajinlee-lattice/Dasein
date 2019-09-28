package com.latticeengines.workflowapi.service.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;

import com.amazonaws.services.batch.model.JobStatus;
import com.latticeengines.aws.emr.EMRService;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.exposed.service.JobCacheService;
import com.latticeengines.yarn.exposed.service.EMREnvService;


public class WorkflowJobStatusChangeCallable implements Callable<Boolean> {
    private static final Logger log = LoggerFactory.getLogger(WorkflowJobStatusChangeCallable.class);

    private static final long period = TimeUnit.HOURS.toMillis(72);
    private WorkflowJobEntityMgr workflowJobEntityMgr;
    private EMREnvService emrEnvService;
    private EMRService emrService;
    private JobCacheService jobCacheService;

    public WorkflowJobStatusChangeCallable(Builder builder) {
        this.workflowJobEntityMgr = builder.workflowJobEntityMgr;
        this.emrEnvService = builder.emrEnvService;
        this.emrService = builder.emrService;
        this.jobCacheService = builder.jobCacheService;
    }

    @Override
    public Boolean call() throws Exception {
        List<String> statuses = Arrays.asList(JobStatus.PENDING.name(), JobStatus.RUNNING.name());
        List<WorkflowJob> jobs = workflowJobEntityMgr.findByStatuses(statuses);
        if (CollectionUtils.isEmpty(jobs)) {
            log.info("workflow job with status pending and runnung is empty");
            return true;
        }
        for (WorkflowJob job : jobs) {
            if (job.getStartTimeInMillis() == null) {
                log.info(String.format("start time for work flow job %s is empty, skip this job.", job.getPid()));
                continue;
            }

            long startTime = job.getStartTimeInMillis();
            if (System.currentTimeMillis() - startTime > period) {
                String clusterId = job.getEmrClusterId();
                if (StringUtils.isBlank(clusterId)) {
                    log.info(String.format("cluster id for workflow job %s is empty, skip this job.", job.getPid()));
                    continue;
                }

                String appId = job.getApplicationId();
                if (StringUtils.isBlank(appId)) {
                    log.info(String.format("app id for workflow job %s is empty, skip this job.", job.getPid()));
                    continue;
                }

                log.info("begin checking cluster state.");
                if (!emrService.isActive(clusterId)) {
                    log.info(String.format("Emr cluster has been shut down for job %s with cluster id %s.",
                            job.getPid(), clusterId));
                    updateStatusToFailed(job);
                    continue;
                }
                log.info(String.format("trying to getting client by cluster %s", clusterId));
                RetryTemplate template = RetryUtils.getRetryTemplate(3);
                template.execute(context -> {
                    try {
                        try (YarnClient yarnClient = emrEnvService.getYarnClient(clusterId)) {
                            yarnClient.start();
                            ApplicationId applicationId = ApplicationIdUtils.toApplicationIdObj(appId);
                            ApplicationReport appReport = yarnClient.getApplicationReport(applicationId);
                            YarnApplicationState state = appReport.getYarnApplicationState();
                            log.info(String.format("begin to deal with workflow %s, %s, %s.", job.getPid(), clusterId,
                                    state.name()));
                            if (YarnApplicationState.FAILED.equals(state)
                                    || YarnApplicationState.KILLED.equals(state)) {
                                updateStatusToFailed(job);
                            }
                            return true;
                        }
                    } catch (IOException | YarnException e) {
                        log.info("internal error" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });
            }

        }

        return true;
    }

    private void updateStatusToFailed(WorkflowJob job) {
        log.info(String.format("update status to failed for workflow job.", job.getPid()));
        job.setStatus(JobStatus.FAILED.name());
        workflowJobEntityMgr.update(job);
        if (job.getWorkflowId() != null) {
            log.info(String.format("evict cache for workflow job %s.", job.getPid()));
            jobCacheService.evictByWorkflowIds(Collections.singletonList(job.getWorkflowId()));
        }
    }

    public static class Builder {
        private WorkflowJobEntityMgr workflowJobEntityMgr;
        private EMREnvService emrEnvService;
        private EMRService emrService;
        private JobCacheService jobCacheService;

        public Builder workflowJobEntityMgr(WorkflowJobEntityMgr workflowJobEntityMgr) {
            this.workflowJobEntityMgr = workflowJobEntityMgr;
            return this;
        }

        public Builder emrEnvService(EMREnvService emrEnvService) {
            this.emrEnvService = emrEnvService;
            return this;
        }

        public Builder jobCacheService(JobCacheService jobCacheService) {
            this.jobCacheService = jobCacheService;
            return this;
        }

        public Builder emrService(EMRService emrService) {
            this.emrService = emrService;
            return this;
        }

        public void builder() {
        }

    }
}
