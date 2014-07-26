package com.latticeengines.dataplatform.service.impl.watchdog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.client.yarn.ContainerProperty;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

@Component("resubmitPreemptedJobsWithThrottling")
public class ResubmitPreemptedJobsWithThrottling extends WatchdogPlugin {
    private static final Log log = LogFactory.getLog(ResubmitPreemptedJobsWithThrottling.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${dataplatform.yarn.job.basedir}")
    private String hdfsJobBaseDir;

    public ResubmitPreemptedJobsWithThrottling() {
        register(this);
    }

    @Override
    public void run(JobExecutionContext context) throws JobExecutionException {
        ThrottleConfiguration latestConfig = throttleConfigurationEntityMgr.getLatestConfig();
        List<Job> jobsToKill = getJobsToKill(latestConfig);
        if (jobsToKill.size() != 0) {
            log.info("Received request to kill " + jobsToKill.size() + " applications");
        }

        // resubmit preempted jobs excluding jobsToKill
        int jobsResubmittedCount = resubmitPreemptedJobs(jobsToKill);
        if (jobsResubmittedCount > 0) {
            log.info(jobsResubmittedCount + " applications resubmitted, going to sleep");
        }

        // kill jobs
        int jobsKilledCount = throttle(latestConfig, jobsToKill);
        if (jobsKilledCount > 0) {
            log.info(jobsKilledCount + " applications killed, going to sleep");
        }
    }

    private int resubmitPreemptedJobs(List<Job> jobsToExcludeFromResubmission) {
        Set<String> jobIdsToExcludeFromResubmission = new HashSet<String>();
        for (Job job : jobsToExcludeFromResubmission) {
            jobIdsToExcludeFromResubmission.add(job.getId());
        }
        List<String> appIds = new ArrayList<String>();
        for (AppInfo appInfo : yarnService.getPreemptedApps()) {
            String appId = appInfo.getAppId();
            // if P0, resubmit immediately with no delay. If any other
            // priorities, delay by some latency
            if (!jobIdsToExcludeFromResubmission.contains(appId)
                    && (appInfo.getQueue().contains("Priority0") || System.currentTimeMillis()
                            - appInfo.getFinishTime() > retryWaitTime)) {
                appIds.add(appId);
            }
        }
        List<Job> jobsToResubmit = jobEntityMgr.findAllByObjectIds(appIds);
        for (Job job : jobsToResubmit) {
            jobService.resubmitPreemptedJob(job);
        }

        return jobsToResubmit.size();
    }

    private List<Job> getJobsToKill(ThrottleConfiguration config) {
        List<Job> jobsToKill = new ArrayList<Job>();

        if (config != null) {
            int cutoffIndex = config.getJobRankCutoff();
            List<Job> runningJobs = jobEntityMgr.findAllByObjectIds(getRunningJobIds());
            Map<Long, Integer> modelToJobCounter = new HashMap<>();

            for (Job job : runningJobs) {
                Long modelId = job.getModel().getPid();
                Integer jobCounter = modelToJobCounter.get(modelId);
                if (jobCounter == null) {
                    jobCounter = 0;
                }
                jobCounter += 1;
                modelToJobCounter.put(modelId, jobCounter);
                if (jobCounter >= cutoffIndex) {
                    if (log.isDebugEnabled()) {
                        log.debug("Finding job [over the rank cutoff]: " + job.getId() + " for model " + modelId);
                    }
                    jobsToKill.add(job);
                }
            }
        }

        return jobsToKill;
    }

    private List<AppInfo> getAppInfos() {
        AppsInfo appsInfo = yarnService.getApplications("states=" + YarnApplicationState.NEW);
        ArrayList<AppInfo> appInfos = appsInfo.getApps();

        appsInfo = yarnService.getApplications("states=" + YarnApplicationState.NEW_SAVING);
        appInfos.addAll(appsInfo.getApps());

        appsInfo = yarnService.getApplications("states=" + YarnApplicationState.SUBMITTED);
        appInfos.addAll(appsInfo.getApps());

        appsInfo = yarnService.getApplications("states=" + YarnApplicationState.ACCEPTED);
        appInfos.addAll(appsInfo.getApps());

        appsInfo = yarnService.getApplications("states=" + YarnApplicationState.RUNNING);
        appInfos.addAll(appsInfo.getApps());
        Collections.sort(appInfos, new Comparator<AppInfo>() {

            @Override
            public int compare(AppInfo o1, AppInfo o2) {
                return o1.getStartTime() - o2.getStartTime() < 0 ? -1 : 1;
            }
        });
        return appInfos;
    }

    private List<String> getRunningJobIds() {
        List<AppInfo> appInfos = getAppInfos();
        List<String> runningJobIds = new ArrayList<String>();
        for (AppInfo appInfo : appInfos) {
            runningJobIds.add(appInfo.getAppId());
        }
        return runningJobIds;
    }

    // kill jobs specified
    private int throttle(ThrottleConfiguration config, List<Job> jobs) {
        Set<String> runningJobIds = new HashSet<String>(getRunningJobIds());

        List<String> appsKilled = new ArrayList<String>();

        if (config != null && config.isEnabled() && config.isImmediate()) {
            for (Job job : jobs) {
                String jobId = job.getId();
                if (runningJobIds.contains(jobId)) {
                    log.info("Killing job " + jobId);
                    try {
                        jobService.killJob(job.getAppId());
                        appsKilled.add(jobId);
                    } catch (Exception e) {
                        log.warn("Cannot kill job " + jobId, e);
                    }
                }
            }
        }

        // clean up job directories
        List<Job> jobsKilled = jobEntityMgr.findAllByObjectIds(appsKilled);
        for (Job job : jobsKilled) {
            String dir = hdfsJobBaseDir + "/" + job.getContainerPropertiesObject().get(ContainerProperty.JOBDIR.name());
            try {
                HdfsUtils.rmdir(yarnConfiguration, dir);
            } catch (Exception e) {
                log.warn("Could not delete job dir " + dir + " due to exception:\n" + ExceptionUtils.getStackTrace(e));
            }
        }

        return appsKilled.size();
    }
}
