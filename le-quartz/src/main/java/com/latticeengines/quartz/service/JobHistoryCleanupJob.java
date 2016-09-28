package com.latticeengines.quartz.service;

import com.latticeengines.quartzclient.entitymanager.JobHistoryEntityMgr;
import org.quartz.*;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.quartz.QuartzJobBean;

public class JobHistoryCleanupJob extends QuartzJobBean {

    public static final String RETAININGDAYS = "RetainingDays";

    private JobHistoryEntityMgr jobHistoryEntityMgr;

    public void init(ApplicationContext appCtx) {
        jobHistoryEntityMgr = (JobHistoryEntityMgr) appCtx.getBean("jobHistoryEntityMgr");
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        SchedulerContext sc = null;
        try {
            sc = context.getScheduler().getContext();
        } catch (SchedulerException e) {
            return;
        }
        ApplicationContext appCtx = (ApplicationContext) sc.get("applicationContext");
        init(appCtx);
        JobDataMap data = context.getJobDetail().getJobDataMap();
        int retainingDays = data.getInt(RETAININGDAYS);
        jobHistoryEntityMgr.deleteOldJobHistory(retainingDays);
    }
}
