package com.latticeengines.quartz.service;

import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerContext;
import org.quartz.SchedulerException;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.quartzclient.entitymanager.JobHistoryEntityMgr;

public class JobHistoryCleanupJob extends QuartzJobBean {

    public static final String RETAININGDAYS = "RetainingDays";

    private JobHistoryEntityMgr jobHistoryEntityMgr;

    private void init(ApplicationContext appCtx) {
        jobHistoryEntityMgr = (JobHistoryEntityMgr) appCtx.getBean("jobHistoryEntityMgr");
    }

    @Override
    protected void executeInternal(JobExecutionContext context) {
        SchedulerContext sc;
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
