package com.latticeengines.quartzclient.service.impl;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.domain.exposed.quartz.QuartzJobArguments;
import com.latticeengines.domain.exposed.quartz.TriggeredJobInfo;
import com.latticeengines.domain.exposed.quartz.TriggeredJobStatus;
import com.latticeengines.quartzclient.entitymanager.core.BaseJobHistoryEntityMgr;
import com.latticeengines.quartzclient.qbean.QuartzJobBean;
import com.latticeengines.quartzclient.service.QuartzJobService;

@ComponentScan({ "com.latticeengines.quartzclient.dao", "com.latticeengines.quartzclient.entitymanager.core",
        "com.latticeengines.quartzclient.entitymanager.impl.core" })
@Component("quartzJobService")
public class QuartzJobServiceImpl implements QuartzJobService {

    private static final Logger log = LoggerFactory.getLogger(QuartzJobServiceImpl.class);

    private AsyncListenableTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();

    private static Map<String, Boolean> jobActives = new HashMap<String, Boolean>();

    private static final String QUARTZ_EXECUTION_HOST = "QUARTZ_EXECUTION_HOST";

    @Autowired
    private BaseJobHistoryEntityMgr jobHistoryEntityMgr;

    @Autowired
    private ApplicationContext appCtx;

    @Override
    public TriggeredJobInfo runJob(QuartzJobArguments jobArgs) {
        String predefinedJobType = jobArgs.getPredefinedJobType();
        QuartzJobBean jobBean = (QuartzJobBean) appCtx.getBean(predefinedJobType);
        if (jobBean == null) {
            log.error("Can not find the bean related to the predefined job type!");
            throw new LedpException(LedpCode.LEDP_30001, new String[]{predefinedJobType});
        } else {
            return runJobInternal(jobArgs, jobBean.getCallable(jobArgs.getJobArguments()));
        }
    }

    private TriggeredJobInfo runJobInternal(QuartzJobArguments jobArgs,
            Callable<Boolean> callable) {
        ListenableFuture<Boolean> task = taskExecutor.submitListenable(callable);
        final String jobId = Integer.toString(task.hashCode());
        final String tenantId = jobArgs.getTenantId();
        final String jobName = jobArgs.getJobName();
        final String jobType = jobArgs.getPredefinedJobType();
        final String jobKey = tenantId + jobName + jobType;
        jobActives.put(jobKey, true);
        task.addCallback(new ListenableFutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {
                try {
                    log.debug(String.format("On Success for job %s", jobName));
                    jobActives.put(jobKey, false);
                    JobHistory jobHistory =getJobHistoryWithRetries(tenantId, jobName, jobId);
                    jobHistory.setTriggeredJobStatus(TriggeredJobStatus.SUCCESS);
                    jobHistoryEntityMgr.updateJobHistory(jobHistory);
                    log.debug("Updated job status to success");
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
                log.debug("Quartz task complete!");
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    log.info(String.format("On Failure for job %s", jobName));
                    jobActives.put(jobKey, false);
                    JobHistory jobHistory =getJobHistoryWithRetries(tenantId, jobName, jobId);
                    jobHistory.setTriggeredJobStatus(TriggeredJobStatus.FAIL);
                    jobHistory.setErrorMessage(t.getMessage());
                    jobHistoryEntityMgr.updateJobHistory(jobHistory);
                    log.debug("Updated job status to fail");
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
                log.error(t.getMessage());
            }
        });

        TriggeredJobInfo triggeredJobInfo = new TriggeredJobInfo();
        triggeredJobInfo.setJobHandle(jobId);
        triggeredJobInfo.setExecutionHost(getExecutionHost());
        return triggeredJobInfo;
    }

    private JobHistory getJobHistoryWithRetries(String tenantId, String jobName, String jobId) {
        RetryTemplate retry = RetryUtils.getRetryTemplate(10);
        return retry.execute(context -> {
            if (context.getRetryCount() > 0) {
                log.info("Attempt=" + (context.getRetryCount() + 1) //
                        + ": retry getting job history for job " + jobName + " : " + jobId);
            }
            JobHistory toReturn = jobHistoryEntityMgr.getJobHistory(tenantId, jobName,
                    jobId);
            if (toReturn == null) {
                throw new IllegalStateException("Job has not been created in DB after " //
                        + (context.getRetryCount() + 1) + " retries, retry later.");
            } else {
                return toReturn;
            }
        });
    }

    @Override
    public Boolean hasActiveJob(QuartzJobArguments jobArgs) {
        String tenantId = jobArgs.getTenantId();
        String jobName = jobArgs.getJobName();
        String jobType = jobArgs.getPredefinedJobType();
        String jobKey = tenantId + jobName + jobType;
        if (jobActives.containsKey(jobKey)) {
            return jobActives.get(jobKey);
        } else {
            return false;
        }
    }

    @Override
    public Boolean jobBeanExist(QuartzJobArguments jobArgs) {
        String jobType = jobArgs.getPredefinedJobType();
        QuartzJobBean jobBean = (QuartzJobBean) appCtx.getBean(jobType);
        if (jobBean == null) {
            log.error("Can not find the bean related to the predefined job type!");
            return false;
        } else {
            return true;
        }
    }

    private String getExecutionHost() {
        String host = System.getenv(QUARTZ_EXECUTION_HOST);
        if (StringUtils.isEmpty(host)) {
            return getHostAddress();
        } else {
            return host;
        }
    }

    private String getHostAddress() {
        String hostName = "";
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new LedpException(LedpCode.LEDP_30002, e);
        }
        return hostName;
    }
}
