package com.latticeengines.quartz.service;

import java.net.URI;
import java.util.Date;

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

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.domain.exposed.quartz.PredefinedJobArguments;
import com.latticeengines.domain.exposed.quartz.TriggeredJobInfo;
import com.latticeengines.domain.exposed.quartz.TriggeredJobStatus;
import com.latticeengines.quartzclient.entitymanager.ActiveStackEntityMgr;
import com.latticeengines.quartzclient.entitymanager.JobHistoryEntityMgr;

public class PreDefinedServerJob extends QuartzJobBean {

    private static final Log log = LogFactory.getLog(PreDefinedServerJob.class);

    public static final String DESTURL = "DestUrl";
    public static final String JOBARGUMENTS = "JobArguments";
    public static final String JOBTYPE = "JobType";
    public static final String TIMEOUT = "Timeout";
    public static final String QUERYAPI = "QueryApi";

    private JobHistoryEntityMgr jobHistoryEntityMgr;

    private ActiveStackEntityMgr activeStackEntityMgr;

    private String currentStack;

    private RestTemplate restTemplate = new RestTemplate();

    public void init(ApplicationContext appCtx) {
        jobHistoryEntityMgr = (JobHistoryEntityMgr) appCtx.getBean("jobHistoryEntityMgr");
        activeStackEntityMgr = (ActiveStackEntityMgr) appCtx.getBean("activeStackEntityMgr");
        try {
            currentStack = PropertyUtils.getProperty("quartz.current.stack");
        } catch (Exception e) {
            currentStack = null;
        }
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

        String activeStack = activeStackEntityMgr.getActiveStack();
        if (activeStack != null && activeStack.equals(currentStack)) {

            JobDataMap data = context.getJobDetail().getJobDataMap();
            String url = data.getString(DESTURL);
            String jobType = data.getString(JOBTYPE);
            String tenantId = context.getJobDetail().getKey().getGroup();
            String jobName = context.getJobDetail().getKey().getName();
            String queryApi = data.getString(QUERYAPI);

            PredefinedJobArguments jobArgs = new PredefinedJobArguments();
            jobArgs.setPredefinedJobType(jobType);
            jobArgs.setJobName(jobName);
            jobArgs.setTenantId(tenantId);

            int jobTimeout = data.getInt(TIMEOUT);

            if (checkAllJobFinished(jobArgs, jobTimeout, queryApi)) {
                JobHistory jobHistory = new JobHistory();
                try {
                    jobHistory.setJobName(jobName);
                    jobHistory.setTenantId(tenantId);
                    jobHistory.setTriggeredTime(new Date());
                    TriggeredJobInfo triggeredJobInfo = restTemplate.postForObject(url, jobArgs,
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
        }

    }

    private Boolean checkAllJobFinished(PredefinedJobArguments jobArgs, int timeout, String queryApi) {
        JobHistory jobHistory = jobHistoryEntityMgr
                .getRecentUnfinishedJobHistory(jobArgs.getTenantId(), jobArgs.getJobName());
        if (jobHistory != null) {
            Date now = new Date(System.currentTimeMillis());
            int elapsedSeconds = (int) (now.getTime() - jobHistory.getTriggeredTime().getTime()) / 1000;
            if (elapsedSeconds > timeout) {
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

    private Boolean checkJobActive(PredefinedJobArguments jobArgs, URI queryUrl) {
        Boolean jobActive = false;
        try {
            jobActive = restTemplate.postForObject(queryUrl, jobArgs, Boolean.class);
        } catch (Exception e) {
            jobActive = false;
        }
        return jobActive;

    }
}
