package com.latticeengines.quartz.service;

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
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.domain.exposed.quartz.TriggeredJobStatus;
import com.latticeengines.quartz.entitymanager.JobHistoryEntityMgr;

@Component("workFlowJob")
public class WorkFlowJob extends QuartzJobBean {

    private static final Log log = LogFactory.getLog(WorkFlowJob.class);

    public static final String DESTURL = "DestUrl";
    public static final String JOBARGUMENTS = "JobArguments";

    private JobHistoryEntityMgr jobHistoryEntityMgr;

    private RestTemplate restTemplate = new RestTemplate();

    public void init(ApplicationContext appCtx) {
        jobHistoryEntityMgr = (JobHistoryEntityMgr) appCtx.getBean("jobHistoryEntityMgr");
    }

    @Override
    protected void executeInternal(JobExecutionContext context)
            throws JobExecutionException {
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
        String jobArguments = data.getString(JOBARGUMENTS);
        JobHistory jobHistory = new JobHistory();
        try {
            jobHistory.setJobName(context.getJobDetail().getKey().getName());
            jobHistory.setTenantId(context.getJobDetail().getKey().getGroup());
            jobHistory.setTriggeredTime(new Date());
            String jobHandle = restTemplate
                    .postForObject(url, jobArguments, String.class);
            jobHistory.setTriggeredJobHandle(jobHandle);
            jobHistory.setTriggeredJobStatus(TriggeredJobStatus.SUCCESS);
        } catch (RestClientException e) {
            log.error(e.getMessage());
            jobHistory.setTriggeredJobStatus(TriggeredJobStatus.FAIL);
            jobHistory.setErrorMessage(e.getMessage());
        }
        jobHistoryEntityMgr.saveJobHistory(jobHistory);
    }
}
