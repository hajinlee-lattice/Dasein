package com.latticeengines.quartz.service;

import java.net.URI;
import java.util.Date;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.domain.exposed.quartz.QuartzJobArguments;
import com.latticeengines.domain.exposed.quartz.TriggeredJobInfo;
import com.latticeengines.domain.exposed.quartz.TriggeredJobStatus;
import com.latticeengines.quartzclient.entitymanager.JobHistoryEntityMgr;

public class CustomQuartzJob extends QuartzJobBean {

    private static final Log log = LogFactory.getLog(CustomQuartzJob.class);

    public static final String DESTURL = "DestUrl";
    public static final String SECONDARYDESTURL = "SecondaryDestUrl";
    public static final String JOBARGUMENTS = "JobArguments";
    public static final String JOBTYPE = "JobType";
    public static final String TIMEOUT = "Timeout";
    public static final String QUERYAPI = "QueryApi";

    private JobHistoryEntityMgr jobHistoryEntityMgr;

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    public void init(ApplicationContext appCtx) {
        jobHistoryEntityMgr = (JobHistoryEntityMgr) appCtx.getBean("jobHistoryEntityMgr");
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        SchedulerContext sc = null;
        try {
            sc = context.getScheduler().getContext();
        } catch (SchedulerException e) {
            log.error(e.getMessage(), e);
        }
        ApplicationContext appCtx = (ApplicationContext) sc.get("applicationContext");
        init(appCtx);

        JobDataMap data = context.getJobDetail().getJobDataMap();
        String url = data.getString(DESTURL);
        String secondaryUrl = data.getString(SECONDARYDESTURL);
        String jobType = data.getString(JOBTYPE);
        String tenantId = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();
        String queryApi = data.getString(QUERYAPI);
        String quartzJobArguments = data.getString(JOBARGUMENTS);

        QuartzJobArguments jobArgs = new QuartzJobArguments();
        jobArgs.setPredefinedJobType(jobType);
        jobArgs.setJobName(jobName);
        jobArgs.setTenantId(tenantId);
        jobArgs.setJobArguments(quartzJobArguments);

        int jobTimeout = data.getInt(TIMEOUT);
        JobHistory lastJobHistory = jobHistoryEntityMgr.getLastJobHistory(tenantId, jobName);
        if (checkAllJobFinished(lastJobHistory, jobArgs, jobTimeout, queryApi)) {
            JobHistory jobHistory = new JobHistory();
            jobHistory.setJobName(jobName);
            jobHistory.setTenantId(tenantId);
            jobHistory.setTriggeredTime(new Date());
            jobHistory.setTriggeredJobStatus(TriggeredJobStatus.TRIGGERED);
            if (lastJobHistory == null) {
                jobHistory.setSequenceId((long)0);
            } else {
                jobHistory.setSequenceId(lastJobHistory.getSequenceId() + 1);
            }
            jobHistory.setJobIdentifier(String.format("%s:%s:%s", tenantId, jobName,
                    jobHistory.getSequenceId().toString()));
            try {
                jobHistoryEntityMgr.createJobHistory(jobHistory);
            } catch (Exception e) {
                log.error("One of the quartz clusters has already triggered the job.");
                return;
            }
            if (checkLastJobFailed(lastJobHistory)) {
                triggerJobWithSecondaryUrl(jobHistory, jobArgs, secondaryUrl);
            } else {
                triggerJobWithPrimaryUrl(jobHistory, jobArgs, url, secondaryUrl);
            }
        } else {
            log.info(String.format("%s was not triggered because the former job is running.", jobName));
        }
    }

    private void triggerJobWithPrimaryUrl(JobHistory jobHistory, QuartzJobArguments jobArgs, String url, String
            secondaryUrl) {
        try {
            TriggeredJobInfo triggeredJobInfo = restTemplate.postForObject(url,
                    jobArgs,
                    TriggeredJobInfo.class);
            String jobHandle = "";
            String executionHost = "";
            if (triggeredJobInfo != null) {
                jobHandle = triggeredJobInfo.getJobHandle();
                executionHost = triggeredJobInfo.getExecutionHost();
            } else {
                log.error("Triggered job info is null!");
            }
            jobHistory.setTriggeredJobHandle(jobHandle);
            jobHistory.setExecutionHost(executionHost);
            jobHistory.setTriggeredJobStatus(TriggeredJobStatus.START);
        } catch (RestClientException e) {
            try {
                TriggeredJobInfo triggeredJobInfo = restTemplate.postForObject(secondaryUrl,
                        jobArgs,
                        TriggeredJobInfo.class);
                String jobHandle = "";
                String executionHost = "";
                if (triggeredJobInfo != null) {
                    jobHandle = triggeredJobInfo.getJobHandle();
                    executionHost = triggeredJobInfo.getExecutionHost();
                } else {
                    log.error("Triggered job info is null!");
                }
                jobHistory.setTriggeredJobHandle(jobHandle);
                jobHistory.setExecutionHost(executionHost);
                jobHistory.setTriggeredJobStatus(TriggeredJobStatus.START);
            } catch (RestClientException e2) {
                jobHistory.setTriggeredJobStatus(TriggeredJobStatus.FAIL);
                jobHistory.setErrorMessage(e2.getMessage());
            }
        }
        jobHistoryEntityMgr.saveJobHistory(jobHistory);
    }

    private void triggerJobWithSecondaryUrl(JobHistory jobHistory, QuartzJobArguments jobArgs, String
            secondaryUrl) {
        try {
            TriggeredJobInfo triggeredJobInfo = restTemplate.postForObject(secondaryUrl,
                    jobArgs,
                    TriggeredJobInfo.class);
            String jobHandle = "";
            String executionHost = "";
            if (triggeredJobInfo != null) {
                jobHandle = triggeredJobInfo.getJobHandle();
                executionHost = triggeredJobInfo.getExecutionHost();
            }
            jobHistory.setTriggeredJobHandle(jobHandle);
            jobHistory.setExecutionHost(executionHost);
            jobHistory.setTriggeredJobStatus(TriggeredJobStatus.START);
        } catch (RestClientException e) {
            jobHistory.setTriggeredJobStatus(TriggeredJobStatus.FAIL);
            jobHistory.setErrorMessage(e.getMessage());
        }
        jobHistoryEntityMgr.saveJobHistory(jobHistory);
    }

    private Boolean checkAllJobFinished(JobHistory jobHistory, QuartzJobArguments jobArgs, int timeout, String
            queryApi) {
        if (jobHistory != null) {
            int timeoutScaler = 1;
            if (jobHistory.getTriggeredJobStatus() != TriggeredJobStatus.START &&
                    jobHistory.getTriggeredJobStatus() != TriggeredJobStatus.TRIGGERED) {
                return true;
            }
            if (jobHistory.getTriggeredJobStatus() == TriggeredJobStatus.TRIGGERED) {
                timeoutScaler = 5;
            }
            Date now = new Date(System.currentTimeMillis());
            int elapsedSeconds = (int) (now.getTime() - jobHistory.getTriggeredTime().getTime()) / 1000;
            if (elapsedSeconds > timeout * timeoutScaler) {
                String executionHost = jobHistory.getExecutionHost();
                URI queryUrl = UriComponentsBuilder
                        .fromUriString(String.format(queryApi, executionHost)).build().toUri();
                if (checkJobActive(jobArgs, queryUrl)) {
                    return false;
                } else {
                    jobHistory.setTriggeredJobStatus(TriggeredJobStatus.TIMEOUT);
                    jobHistoryEntityMgr.updateJobHistory(jobHistory);
                    return true;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    private Boolean checkLastJobFailed(JobHistory jobHistory) {
        if (jobHistory != null) {
            return jobHistory.getTriggeredJobStatus() == TriggeredJobStatus.FAIL;
        } else {
            return false;
        }
    }

    private Boolean checkJobActive(QuartzJobArguments jobArgs, URI queryUrl) {
        Boolean jobActive = false;
        try {
            jobActive = restTemplate.postForObject(queryUrl, jobArgs, Boolean.class);
        } catch (Exception e) {
            log.error(String.format("Cannot get job active status from %s, exception: %s",
                    queryUrl.toString(), e.toString()));
            jobActive = false;
        }
        return jobActive;
    }
}
