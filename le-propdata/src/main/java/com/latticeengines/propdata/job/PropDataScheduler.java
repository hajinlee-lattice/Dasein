package com.latticeengines.propdata.job;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.collection.service.impl.ProgressOrchestrator;
import com.latticeengines.propdata.core.service.ZkConfigurationService;
import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.RawSource;
import com.latticeengines.propdata.core.source.Source;

@Component("propDataScheduler")
public class PropDataScheduler {

    private Scheduler scheduler;
    private Log log = LogFactory.getLog(PropDataScheduler.class);

    @Value("${propdata.job.schedule.dryrun:true}")
    Boolean dryrun;

    @Autowired
    List<RawSource> rawSourceList;

    @Autowired
    List<DerivedSource> derivedSourceList;

    @Autowired
    ZkConfigurationService zkConfigurationService;

    @Autowired
    ProgressOrchestrator progressOrchestrator;

    @PostConstruct
    private void registerJobs() throws SchedulerException {
        scheduler = new StdSchedulerFactory().getScheduler();

        for(RawSource source: rawSourceList) {
            try {
                registerArchiveJob(source);
            } catch (Exception e) {
                log.error("Failed to register scheduler for source " + source.getSourceName());
            }
        }

        for (DerivedSource source: derivedSourceList) {
            try {
                registerRefreshJob(source);
            } catch (Exception e) {
                log.error("Failed to register scheduler for source " + source.getSourceName());
            }
        }

        scheduler.start();
    }

    public void reschedule() {
        List<Source> sources = new ArrayList<Source>(rawSourceList);
        sources.addAll(derivedSourceList);

        for(Source source: sources) {
            try {
                rescheduleSource(source);
            } catch (Exception e) {
                log.error("Failed to reschedule scheduler for source " + source.getSourceName());
            }
        }
    }

    private void registerArchiveJob(RawSource source) throws SchedulerException {
        ArchiveService service = progressOrchestrator.getArchiveService(source);
        if (service != null) {
            JobDetail job = JobBuilder.newJob(ArchiveScheduler.class)
                    .usingJobData("dryrun", dryrun)
                    .build();
            job.getJobDataMap().put("archiveService", service);
            job.getJobDataMap().put("zkConfigurationService", zkConfigurationService);

            scheduler.scheduleJob(job, cronTriggerForSource(source));
        }
    }

    private void registerRefreshJob(DerivedSource source) throws SchedulerException {
        RefreshService service = progressOrchestrator.getRefreshService(source);
        if (service != null) {
            JobDetail job = JobBuilder.newJob(RefreshScheduler.class)
                    .usingJobData("dryrun", dryrun)
                    .build();
            job.getJobDataMap().put("refreshService", service);
            job.getJobDataMap().put("zkConfigurationService", zkConfigurationService);

            scheduler.scheduleJob(job, cronTriggerForSource(source));
        }
    }

    private CronTrigger cronTriggerForSource(Source source) {
        String cron = zkConfigurationService.refreshCronSchedule(source);
        return TriggerBuilder.newTrigger()
                .withIdentity(new TriggerKey(source.getSourceName()))
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .build();
    }

    private void rescheduleSource(Source source) throws SchedulerException {
        TriggerKey key = new TriggerKey(source.getSourceName());
        CronTrigger oldTrigger = (CronTrigger) scheduler.getTrigger(key);
        if (!zkConfigurationService.refreshCronSchedule(source).equals(oldTrigger.getCronExpression())) {
            CronTrigger newTrigger = cronTriggerForSource(source);
            log.info("Reschedule " + source.getSourceName() + " from " + oldTrigger.getCronExpression()
                    + " to " + newTrigger.getCronExpression());
            scheduler.rescheduleJob(key, newTrigger);
        }
    }

}