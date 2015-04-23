package com.latticeengines.dellebi;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import com.latticeengines.dellebi.dataprocess.DailyJob;


public class DellEBIMain { 

    private static final Logger LOGGER = Logger.getLogger(DellEBIMain.class);

    public static void main(String args[]) {

        System.setProperty("org.quartz.properties", System.getProperty("user.dir")
                + "/src/main/resources/quartz.properties");
        DOMConfigurator.configure(System.getProperty("user.dir") + "/src/main/resources/log4j.xml");

        String dellebi_propdir = args[0];
        System.setProperty("DELLEBI_PROPDIR", dellebi_propdir);

        LOGGER.info("DellEBI main starts.");  

        /******************** Start daily refresh with Quartz **************************/

        LOGGER.info("DellEBI daily refresh starts."); 

        JobDetail dailyRefreshJob = JobBuilder.newJob(DailyJob.class).withIdentity("job1", "group1").build();

        // Execute daily refresh on Monday to Friday on 4 PM.
        Trigger dailyRefreshTrigger = TriggerBuilder.newTrigger().withIdentity("trigger1", "group1")
             .withSchedule(CronScheduleBuilder.cronSchedule("0 0/3 13 ? * MON-FRI")).build();

        // Schedule the job.

        try {
            SchedulerFactory sf = new StdSchedulerFactory();
            Scheduler sched = sf.getScheduler();
            sched.start();
            sched.scheduleJob(dailyRefreshJob, dailyRefreshTrigger);
        } catch (SchedulerException e) {
            LOGGER.fatal("Failed to schedule daily refresh.");
            LOGGER.fatal("Failed!", e);
        }
    }
}
