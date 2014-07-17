package com.latticeengines.dataplatform.service.impl.watchdog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
        // resubmit preempted jobs excluding jobsToKill
        resubmitPreemptedJobs(jobsToKill);
        // kill jobs
        throttle(latestConfig, jobsToKill);
    }

    private void resubmitPreemptedJobs(List<Job> jobsToExcludeFromResubmission) {
        Set<String> jobIdsToExcludeFromResubmission = new HashSet<String>();
        for (Job job : jobsToExcludeFromResubmission) {
            jobIdsToExcludeFromResubmission.add(job.getId());
        }
        Set<String> appIds = new HashSet<String>();
        for (AppInfo appInfo : yarnService.getPreemptedApps()) {
            String appId = appInfo.getAppId();
            // if P0, resubmit immediately with no delay. If any other priorities, delay by some latency
            if (!jobIdsToExcludeFromResubmission.contains(appId)
                    && (appInfo.getQueue().contains("Priority0") || System.currentTimeMillis() - appInfo.getFinishTime() > retryWaitTime)) {
                appIds.add(appId);
            }
        }
        Set<Job> jobsToResubmit = jobEntityMgr.findAllByObjectIds(appIds);
        for (Job job : jobsToResubmit) {
            jobService.resubmitPreemptedJob(job);
        }
    }

    private List<Job> getJobsToKill(ThrottleConfiguration config) {
        List<Job> jobsToKill = new ArrayList<Job>();

        if (config != null) {
            int cutoffIndex = config.getJobRankCutoff();

            Set<Job> runningJobs = jobEntityMgr.findAllByObjectIds(getRunningJobIds());
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

    private Set<String> getRunningJobIds() {
        AppsInfo appsInfo = yarnService.getApplications("states=RUNNING");
        ArrayList<AppInfo> appInfos = appsInfo.getApps();
        Set<String> runningJobIds = new HashSet<String>();

        for (AppInfo appInfo : appInfos) {
            runningJobIds.add(appInfo.getAppId());
        }

        return runningJobIds;
    }

    // kill jobs specified
    private void throttle(ThrottleConfiguration config, List<Job> jobs) {
        Set<String> runningJobIds = getRunningJobIds();

        Set<String> appsKilled = new HashSet<String>();

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
        Set<Job> jobsKilled = jobEntityMgr.findAllByObjectIds(appsKilled);
        for (Job job : jobsKilled) {
            String dir = hdfsJobBaseDir + "/" + job.getContainerPropertiesObject().get(ContainerProperty.JOBDIR.name());
            try {
                HdfsUtils.rmdir(yarnConfiguration, dir);
            } catch (Exception e) {
                log.warn("Could not delete job dir " + dir + " due to exception:\n" + ExceptionUtils.getStackTrace(e));
            }
        }
    }
}
