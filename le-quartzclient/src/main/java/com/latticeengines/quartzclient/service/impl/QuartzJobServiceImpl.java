package com.latticeengines.quartzclient.service.impl;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.domain.exposed.quartz.PredefinedJobArguments;
import com.latticeengines.domain.exposed.quartz.TriggeredJobInfo;
import com.latticeengines.domain.exposed.quartz.TriggeredJobStatus;
import com.latticeengines.quartzclient.entitymanager.core.BaseJobHistoryEntityMgr;
import com.latticeengines.quartzclient.mbean.QuartzJobBean;
import com.latticeengines.quartzclient.service.QuartzJobService;

@ComponentScan({ "com.latticeengines.quartzclient.dao", "com.latticeengines.quartzclient.entitymanager.core",
        "com.latticeengines.quartzclient.entitymanager.impl.core" })
@Component("quartzJobService")
public class QuartzJobServiceImpl implements QuartzJobService {

    private static final Log log = LogFactory.getLog(QuartzJobServiceImpl.class);

    private AsyncListenableTaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();

    private Map<String, Callable<Boolean>> callables = new HashMap<String, Callable<Boolean>>();

    private static Map<String, Boolean> jobActives = new HashMap<String, Boolean>();

    @Autowired
    private BaseJobHistoryEntityMgr jobHistoryEntityMgr;

    @Autowired
    private ApplicationContext appCtx;

    @Override
    public TriggeredJobInfo runJob(PredefinedJobArguments jobArgs) {
        String predefinedJobType = jobArgs.getPredefinedJobType();
        if (callables.containsKey(predefinedJobType)) {
            return runJobInternal(jobArgs, callables.get(predefinedJobType));
        } else {
            QuartzJobBean jobBean = (QuartzJobBean) appCtx.getBean(predefinedJobType);
            if (jobBean == null) {
                log.error("Can not find the bean related to the predefined job type!");
                return null;
            } else {
                callables.put(predefinedJobType, jobBean.getCallable());
                return runJobInternal(jobArgs, callables.get(predefinedJobType));
            }
        }
    }

    private TriggeredJobInfo runJobInternal(PredefinedJobArguments jobArgs,
            Callable<Boolean> callable) {
        ListenableFuture<Boolean> task = taskExecutor
                .submitListenable(callable);
        jobActives.put(jobArgs.getPredefinedJobType(), true);
        final String jobId = Integer.toString(task.hashCode());
        final String tenantId = jobArgs.getTenantId();
        final String jobName = jobArgs.getJobName();
        final String predefinedJobType = jobArgs.getPredefinedJobType();
        task.addCallback(new ListenableFutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {
                try {
                    jobActives.put(predefinedJobType, false);
                    JobHistory jobHistory = jobHistoryEntityMgr.getJobHistory(tenantId, jobName,
                            jobId);
                    if (jobHistory != null) {
                        jobHistory.setTriggeredJobStatus(TriggeredJobStatus.SUCCESS);
                        jobHistoryEntityMgr.updateJobHistory(jobHistory);
                        log.info("Updated job status to success");
                    } else {
                        // incase the job finished too soon.
                        Thread.sleep(3000);
                        jobHistory = jobHistoryEntityMgr.getJobHistory(tenantId, jobName, jobId);
                        if (jobHistory != null) {
                            jobHistory.setTriggeredJobStatus(TriggeredJobStatus.SUCCESS);
                            jobHistoryEntityMgr.updateJobHistory(jobHistory);
                            log.info("Updated job status to success");
                        }
                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
                log.info("Quartz task complete!");
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    jobActives.put(predefinedJobType, false);
                    JobHistory jobHistory = jobHistoryEntityMgr.getJobHistory(tenantId, jobName,
                            jobId);
                    if (jobHistory != null) {
                        jobHistory.setTriggeredJobStatus(TriggeredJobStatus.FAIL);
                        jobHistory.setErrorMessage(t.getMessage());
                        jobHistoryEntityMgr.updateJobHistory(jobHistory);
                        log.info("Updated job status to fail");
                    } else {
                        // incase the job finished too soon.
                        Thread.sleep(3000);
                        jobHistory = jobHistoryEntityMgr.getJobHistory(tenantId, jobName, jobId);
                        if (jobHistory != null) {
                            jobHistory.setTriggeredJobStatus(TriggeredJobStatus.FAIL);
                            jobHistory.setErrorMessage(t.getMessage());
                            jobHistoryEntityMgr.updateJobHistory(jobHistory);
                            log.info("Updated job status to fail");
                        }
                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
                log.error(t.getMessage());
            }
        });

        TriggeredJobInfo triggeredJobInfo = new TriggeredJobInfo();
        triggeredJobInfo.setJobHandle(jobId);
        triggeredJobInfo.setExecutionHost(getHostAddress());
        return triggeredJobInfo;
    }

    @Override
    public Boolean hasActiveJob(PredefinedJobArguments jobArgs) {
        if (jobActives.containsKey(jobArgs.getPredefinedJobType())) {
            return jobActives.get(jobArgs.getPredefinedJobType());
        } else {
            return false;
        }
    }

    private String getHostAddress() {
        String hostName = "";
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return hostName;
    }
}
