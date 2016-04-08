package com.latticeengines.propdata.job;

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
import com.latticeengines.propdata.core.service.ServiceFlowsZkConfigService;
import com.latticeengines.propdata.core.source.DataImportedFromDB;
import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.Source;

@Component("propDataScheduler")
public class PropDataScheduler {

    private Scheduler scheduler;
    private Log log = LogFactory.getLog(PropDataScheduler.class);

    @Value("${propdata.job.schedule.dryrun:true}")
    private Boolean dryrun;

    @Autowired
    private List<Source> sourceList;

    @Autowired
    private ServiceFlowsZkConfigService serviceFlowsZkConfigService;

    @Autowired
    private ProgressOrchestrator progressOrchestrator;

    @PostConstruct
    private void registerJobs() throws SchedulerException {
        scheduler = new StdSchedulerFactory().getScheduler();

        for (Source source : sourceList) {
            if (serviceFlowsZkConfigService.refreshJobEnabled(source)) {
                try {
                    registerJob(source);
                } catch (Exception e) {
                    log.error("Failed to register scheduler for source " + source.getSourceName(), e);
                }
            }
        }

        scheduler.start();
    }

    public void reschedule() {
        for (Source source : sourceList) {
            if (serviceFlowsZkConfigService.refreshJobEnabled(source)) {
                try {
                    rescheduleSource(source);
                } catch (Exception e) {
                    log.error("Failed to reschedule scheduler for source " + source.getSourceName(), e);
                }
            }
        }
    }

    private void registerJob(Source source) throws SchedulerException {
        if (source instanceof DataImportedFromDB) {
            registerArchiveJob((DataImportedFromDB) source);
        } else if (source instanceof DerivedSource) {
            registerRefreshJob((DerivedSource) source);
        }
    }

    private void registerArchiveJob(DataImportedFromDB source) throws SchedulerException {
        ArchiveService service = progressOrchestrator.getArchiveService(source);
        if (service != null) {
            JobDetail job = JobBuilder.newJob(ArchiveScheduler.class).usingJobData("dryrun", dryrun).build();
            job.getJobDataMap().put("archiveService", service);
            job.getJobDataMap().put("zkConfigurationService", serviceFlowsZkConfigService);

            scheduler.scheduleJob(job, cronTriggerForSource(source));
        }
    }

    private void registerRefreshJob(DerivedSource source) throws SchedulerException {
        RefreshService service = progressOrchestrator.getRefreshService(source);
        if (service != null) {
            JobDetail job = JobBuilder.newJob(RefreshScheduler.class).usingJobData("dryrun", dryrun).build();
            job.getJobDataMap().put("refreshService", service);
            job.getJobDataMap().put("zkConfigurationService", serviceFlowsZkConfigService);

            scheduler.scheduleJob(job, cronTriggerForSource(source));
        }
    }

    private CronTrigger cronTriggerForSource(Source source) {
        String cron = serviceFlowsZkConfigService.refreshCronSchedule(source);
        return TriggerBuilder.newTrigger().withIdentity(new TriggerKey(source.getSourceName()))
                .withSchedule(CronScheduleBuilder.cronSchedule(cron)).build();
    }

    private void rescheduleSource(Source source) throws SchedulerException {
        TriggerKey key = new TriggerKey(source.getSourceName());
        CronTrigger oldTrigger = (CronTrigger) scheduler.getTrigger(key);
        if (oldTrigger == null) {
            registerJob(source);
        } else if (!serviceFlowsZkConfigService.refreshCronSchedule(source).equals(oldTrigger.getCronExpression())) {
            CronTrigger newTrigger = cronTriggerForSource(source);
            log.info("Reschedule " + source.getSourceName() + " from " + oldTrigger.getCronExpression() + " to "
                    + newTrigger.getCronExpression());
            scheduler.rescheduleJob(key, newTrigger);
        }
    }

}