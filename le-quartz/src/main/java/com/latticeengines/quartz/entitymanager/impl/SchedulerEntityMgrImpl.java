package com.latticeengines.quartz.entitymanager.impl;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.validator.routines.UrlValidator;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.quartz.JobConfig;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.domain.exposed.quartz.JobInfo;
import com.latticeengines.domain.exposed.quartz.JobInfoDetail;
import com.latticeengines.domain.exposed.quartz.JobSource;
import com.latticeengines.domain.exposed.quartz.JobSourceType;
import com.latticeengines.domain.exposed.quartz.QuartzJobArguments;
import com.latticeengines.quartz.entitymanager.SchedulerEntityMgr;
import com.latticeengines.quartz.service.CustomQuartzJob;
import com.latticeengines.quartz.service.JobHistoryCleanupJob;
import com.latticeengines.quartzclient.entitymanager.JobHistoryEntityMgr;
import com.latticeengines.quartzclient.entitymanager.JobSourceEntityMgr;

@Component("schedulerEntityMgr")
public class SchedulerEntityMgrImpl implements SchedulerEntityMgr {

    private static final Log log = LogFactory.getLog(SchedulerEntityMgrImpl.class);

    private static final String JOB_TYPE = "jobType";
    private static final String TRIGGER_SUFFIX = "_trigger";
    private static final String PREDEFINED_JOB_GROUP = "PredefinedJobs";
    private static final String BACKGROUND_JOB_GROUP = "BackgroundJobs";

    @Autowired
    private Scheduler scheduler;

    @Autowired
    private JobHistoryEntityMgr jobHistoryEntityMgr;

    @Autowired
    private JobSourceEntityMgr jobSourceEntityMgr;

    @Autowired
    private ApplicationContext appContext;

    @Value("${quartz.predefined.jobs.enabled}")
    private String enabledPredefinedJobs;

    @Value("${quartz.scheduler.jobs.history.retaining.days:30}")
    private int jobHistoryRetainingDays;

    @Value("${quartz.scheduler.jobs.history.cleanup.trigger:0 0 5 * * ?}")
    private String jobHistoryCleanupJobCronTrigger;

    private RestTemplate restTemplate = HttpClientUtils.newRestTemplate();

    @Override
    public Boolean addJob(String tenantId, JobConfig jobConfig) {
        return addJob(tenantId, jobConfig, JobSourceType.MANUAL, true);
    }

    private Boolean addJob(String tenantId, JobConfig jobConfig, JobSourceType sourceType, boolean checkJobBean) {
        Boolean added = false;
        JobKey jobKey = new JobKey(jobConfig.getJobName(), tenantId);
        try {
            if (scheduler.checkExists(jobKey)) {
                added = false;
            } else {
                if (!checkUrl(jobConfig.getDestUrl())) {
                    throw new LedpException(LedpCode.LEDP_30000, new String[]{jobConfig.getDestUrl()});
                }

                String jobArgs = jobConfig.getJobArguments();
                JSONObject json = new JSONObject(jobArgs);
                String jobType = json.getString(JOB_TYPE);

                if (checkJobBean) {
                    QuartzJobArguments quartzJobArgs = new QuartzJobArguments();
                    quartzJobArgs.setTenantId(tenantId);
                    quartzJobArgs.setJobName(jobConfig.getJobName());
                    quartzJobArgs.setPredefinedJobType(jobType);
                    if (!checkJobBean(jobConfig.getCheckJobBeanUrl(), quartzJobArgs)) {
                        throw new LedpException(LedpCode.LEDP_30001, new String[]{jobType});
                    }
                }

                json.remove(JOB_TYPE);
                String jobArguments = json.toString();
                JobDetail jobDetail = JobBuilder
                        .newJob(com.latticeengines.quartz.service.CustomQuartzJob.class)
                        .withIdentity(jobKey)
                        .build();
                jobDetail.getJobDataMap().put(CustomQuartzJob.DESTURL, jobConfig.getDestUrl());
                jobDetail.getJobDataMap().put(CustomQuartzJob.SECONDARYDESTURL, jobConfig.getSecondaryDestUrl());
                jobDetail.getJobDataMap().put(CustomQuartzJob.QUERYAPI, jobConfig.getQueryApi());
                jobDetail.getJobDataMap().put(CustomQuartzJob.TIMEOUT, jobConfig.getJobTimeout());
                jobDetail.getJobDataMap().put(CustomQuartzJob.JOBTYPE, jobType);
                jobDetail.getJobDataMap().put(CustomQuartzJob.JOBARGUMENTS, jobArguments);
                CronTrigger trigger = TriggerBuilder
                        .newTrigger()
                        .withIdentity(jobConfig.getJobName() + TRIGGER_SUFFIX, tenantId)
                        .withSchedule(CronScheduleBuilder
                                .cronSchedule(jobConfig.getCronTrigger())
                                .withMisfireHandlingInstructionDoNothing())
                        .build();
                scheduler.scheduleJob(jobDetail, trigger);
                updateJobSource(tenantId, jobConfig.getJobName(), sourceType);
                added = true;
            }
        } catch (SchedulerException e) {
            added = false;
            log.error(e.getMessage());
        } catch (JSONException e) {
            added = false;
            log.error(e.getMessage());
        }
        return added;
    }

    @Override
    public Boolean deleteJob(String tenantId, String jobName) {
        Boolean deleted = false;
        try {
            JobKey jobKey = new JobKey(jobName, tenantId);
            if (scheduler.checkExists(jobKey)) {
                scheduler.deleteJob(jobKey);
            }
            deleted = true;
            updateJobSource(tenantId, jobName, JobSourceType.MANUAL);
        } catch (SchedulerException e) {
            deleted = false;
            log.error(e.getMessage());
        }
        return deleted;
    }

    @Override
    public List<JobInfo> listJobs(String tenantId) {
        List<JobInfo> allJobs = new ArrayList<JobInfo>();
        try {
            for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(tenantId))) {
                String jobName = jobKey.getName();
                JobInfo jobInfo = new JobInfo();
                jobInfo.setJobName(jobName);
                jobInfo.setTenantId(tenantId);
                allJobs.add(jobInfo);
            }
        } catch (SchedulerException e) {
            allJobs.clear();
            log.error(e.getMessage());
        }
        return allJobs;
    }

    @Override
    public List<JobInfo> listAllJobs() {
        List<JobInfo> allJobs = new ArrayList<JobInfo>();
        try {
            for (String groupName : scheduler.getJobGroupNames()) {
                for (JobKey jobKey : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(groupName))) {
                    String jobName = jobKey.getName();
                    JobInfo jobInfo = new JobInfo();
                    jobInfo.setJobName(jobName);
                    jobInfo.setTenantId(groupName);
                    allJobs.add(jobInfo);
                }
            }
        } catch (SchedulerException e) {
            allJobs.clear();
            log.error(e.getMessage());
        }
        return allJobs;
    }

    @SuppressWarnings("unchecked")
    @Override
    public JobInfoDetail getJobDetail(String tenantId, String jobName) {
        JobInfoDetail jobDetail = new JobInfoDetail();
        try {
            JobKey jobKey = new JobKey(jobName, tenantId);
            if (scheduler.checkExists(jobKey)) {
                jobDetail.setJobName(jobKey.getName());
                jobDetail.setTenantId(jobKey.getGroup());
                List<Trigger> triggers = (List<Trigger>) scheduler.getTriggersOfJob(jobKey);
                jobDetail.setNextTriggerTime(triggers.get(0).getNextFireTime());
                List<JobHistory> jobHistories = jobHistoryEntityMgr.getJobHistory(tenantId, jobName);
                jobDetail.setHistoryJobs(jobHistories);
            }
        } catch (SchedulerException e) {
            log.error(e.getMessage());
        }
        return jobDetail;
    }

    @Override
    public Boolean pauseAllJobs() {
        Boolean paused = false;
        try {
            scheduler.pauseAll();
            paused = true;
        } catch (SchedulerException e) {
            paused = false;
            log.error(e.getMessage());
        }
        return paused;
    }

    @Override
    public Boolean resumeAllJobs() {
        Boolean resumed = false;
        try {
            scheduler.resumeAll();
            resumed = true;
        } catch (SchedulerException e) {
            resumed = false;
            log.error(e.getMessage());
        }
        return resumed;
    }

    private Boolean checkUrl(String url) {
        String[] schemes = { "http", "https" };
        UrlValidator urlValidator = new UrlValidator(schemes, UrlValidator.ALLOW_LOCAL_URLS);
        return urlValidator.isValid(url);
    }

    private boolean checkJobBean(String url, QuartzJobArguments jobArgs) {
        Boolean jobBeanExist = false;
        try {
            URI queryUrl = UriComponentsBuilder.fromUriString(url).build().toUri();
            jobBeanExist = restTemplate.postForObject(queryUrl, jobArgs, Boolean.class);
        } catch (Exception e) {
            jobBeanExist = false;
        }
        return jobBeanExist;
    }

    @SuppressWarnings("unchecked")
    @PostConstruct
    private void addPredefinedJobs() {
        if (enabledPredefinedJobs != null && enabledPredefinedJobs.trim().length() > 0) {
            log.info("Add predefined jobs.");
            HashSet<String> enabledJobs = new HashSet<>();
            StringTokenizer st = new StringTokenizer(enabledPredefinedJobs, ",");
            while (st.hasMoreTokens()) {
                enabledJobs.add(st.nextToken());
            }
            List<JobConfig> jobConfigs = (List<JobConfig>) appContext.getBean("predefinedJobs");
            for (JobConfig jobConfig : jobConfigs) {
                if (enabledJobs.contains(jobConfig.getJobName())) {
                    log.info("Trying to add predefined job " + jobConfig.getJobName());
                    addPredefinedJob(jobConfig);
                } else {
                    log.info("Trying to delete predefined job " + jobConfig.getJobName());
                    deletePredefinedJob(jobConfig);
                }
            }
        } else {
            log.info("Predefined jobs will not added due to there is no job enabled.");
            List<JobConfig> jobConfigs = (List<JobConfig>) appContext.getBean("predefinedJobs");
            for (JobConfig jobConfig : jobConfigs) {
                deletePredefinedJob(jobConfig);
            }
        }
        addJobHistoryCleanupJob();
    }

    private void addJobHistoryCleanupJob() {
        JobKey jobKey = new JobKey("jobHistoryCleanupJob", BACKGROUND_JOB_GROUP);
        try {
            if (scheduler.checkExists(jobKey)) {
                log.info("Job history clean up job already exists");
                return;
            } else {
                JobDetail jobDetail = JobBuilder
                        .newJob(com.latticeengines.quartz.service.JobHistoryCleanupJob.class)
                        .withIdentity(jobKey)
                        .build();
                jobDetail.getJobDataMap().put(JobHistoryCleanupJob.RETAININGDAYS, jobHistoryRetainingDays);
                CronTrigger trigger = TriggerBuilder
                        .newTrigger()
                        .withIdentity("jobHistoryCleanupJob" + TRIGGER_SUFFIX, BACKGROUND_JOB_GROUP)
                        .withSchedule(CronScheduleBuilder
                                .cronSchedule(jobHistoryCleanupJobCronTrigger)
                                .withMisfireHandlingInstructionDoNothing())
                        .build();
                scheduler.scheduleJob(jobDetail, trigger);
            }
        } catch (SchedulerException e) {
            log.error(e.getMessage());
        }
    }

    @Override
    public void addPredefinedJob(JobConfig jobConfig) {
        JobKey jobKey = new JobKey(jobConfig.getJobName(), PREDEFINED_JOB_GROUP);
        try {
            JobSource jobSource = jobSourceEntityMgr.getJobSourceType(PREDEFINED_JOB_GROUP, jobConfig.getJobName());
            if (jobSource == null || jobSource.getSourceType() == JobSourceType.DEFAULT) {
                if (scheduler.checkExists(jobKey)) {
                    log.info("Deleting job key " + jobKey + " before adding it.");
                    scheduler.deleteJob(jobKey);
                }
                addJob(PREDEFINED_JOB_GROUP, jobConfig, JobSourceType.DEFAULT, false);
                return;
            } else {
                if (scheduler.checkExists(jobKey)) {
                    log.info(String.format("Job %s already added manually", jobConfig.getJobName()));
                } else {
                    log.info(String.format("Job %s was deleted manually", jobConfig.getJobName()));
                }
            }
        } catch (SchedulerException e) {
            log.error(e.getMessage());
        }
    }

    private void deletePredefinedJob(JobConfig jobConfig) {
        JobKey jobKey = new JobKey(jobConfig.getJobName(), PREDEFINED_JOB_GROUP);
        try {
            JobSource jobSource = jobSourceEntityMgr.getJobSourceType(PREDEFINED_JOB_GROUP, jobConfig.getJobName());
            if (jobSource == null || jobSource.getSourceType() == JobSourceType.DEFAULT) {
                if (scheduler.checkExists(jobKey)) {
                    scheduler.deleteJob(jobKey);
                }
            }
        } catch (SchedulerException e) {
            log.error(e.getMessage());
        }
    }

    private void updateJobSource(String tenantId, String jobName, JobSourceType sourceType) {
        JobSource jobSource = jobSourceEntityMgr.getJobSourceType(tenantId, jobName);
        if (jobSource == null) {
            jobSource = new JobSource();
            jobSource.setTenantId(tenantId);
            jobSource.setJobName(jobName);
            jobSource.setSourceType(sourceType);
        } else {
            jobSource.setSourceType(sourceType);
        }
        jobSourceEntityMgr.saveJobSource(jobSource);
    }
}
