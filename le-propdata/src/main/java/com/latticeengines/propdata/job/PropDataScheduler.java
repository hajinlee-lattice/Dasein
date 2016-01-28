package com.latticeengines.propdata.job;

import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
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

@Component
public class PropDataScheduler {

    private Scheduler scheduler;
    private Log log = LogFactory.getLog(PropDataScheduler.class);

    @Value("${propdata.job.default.schedule}")
    String defaultCron;

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

    private Trigger cronTriggerForSource(Source source) {
        String cron = zkConfigurationService.refreshCronSchedule(source);
        if (StringUtils.isEmpty(cron)) { cron = defaultCron; }
        return TriggerBuilder.newTrigger()
                .withIdentity(source.getSourceName())
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .build();
    }

}