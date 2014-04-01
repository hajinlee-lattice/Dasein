package com.latticeengines.dataplatform.service.impl.watchdog;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.latticeengines.dataplatform.exposed.domain.Job;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;

public class ResubmitPreemptedJobsWithThrottling extends WatchdogPlugin {
    private static final Log log = LogFactory.getLog(ResubmitPreemptedJobsWithThrottling.class);

    public ResubmitPreemptedJobsWithThrottling() {
        register(this);
    }

    @Override
    public void run(JobExecutionContext context) throws JobExecutionException {
        ThrottleConfiguration latestConfig = throttleConfigurationEntityMgr.getLatestConfig();
        List<Job> jobsToKill = getJobsToKill(latestConfig);
        resubmitPreemptedJobs(jobsToKill);
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
        Set<Job> jobsToResubmit = jobEntityMgr.getByIds(appIds);
        for (Job job : jobsToResubmit) {
            jobService.resubmitPreemptedJob(job);
        }
    }

    private List<Job> getJobsToKill(ThrottleConfiguration config) {
        List<Job> jobs = new ArrayList<Job>();

        if (config != null) {
            int cutoffIndex = config.getJobRankCutoff();

            List<Model> models = modelEntityMgr.getAll();
            for (Model model : models) {
                List<Job> ownedJobs = model.getJobs();
                for (int i = 1; i <= ownedJobs.size(); i++) {
                    if (i >= cutoffIndex) {
                        log.info("Adding job " + ownedJobs.get(i - 1).getId() + " for model " + model.getId());
                        jobs.add(ownedJobs.get(i - 1));
                    }
                }

            }

        }
        return jobs;
    }

    private void throttle(ThrottleConfiguration config, List<Job> jobs) {
        if (config != null && config.isEnabled() && config.isImmediate()) {
            for (Job job : jobs) {
                log.info("Killing job " + job.getId());
                jobService.killJob(job.getAppId());
            }
        }
    }
}
