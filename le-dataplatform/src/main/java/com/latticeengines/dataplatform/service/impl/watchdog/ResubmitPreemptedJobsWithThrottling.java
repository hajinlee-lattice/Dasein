package com.latticeengines.dataplatform.service.impl.watchdog;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

@Component("resubmitPreemptedJobsWithThrottling")
public class ResubmitPreemptedJobsWithThrottling extends WatchdogPlugin {
    private static final Log log = LogFactory.getLog(ResubmitPreemptedJobsWithThrottling.class);

    public ResubmitPreemptedJobsWithThrottling() {
        register(this);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
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
        List<Job> jobs = new ArrayList<Job>();

        if (config != null) {
            int cutoffIndex = config.getJobRankCutoff();

            List<Model> models = modelEntityMgr.findAll();  // getAll();
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

    // kill jobs specified
    private void throttle(ThrottleConfiguration config, List<Job> jobs) {
        AppsInfo appsInfo = yarnService.getApplications("states=RUNNING");
        ArrayList<AppInfo> appInfos = appsInfo.getApps();
        Set<String> runningJobIds = new HashSet<String>();
        
        for (AppInfo appInfo : appInfos) {
            runningJobIds.add(appInfo.getAppId());
        }
        if (config != null && config.isEnabled() && config.isImmediate()) {
            for (Job job : jobs) {
                String jobId = job.getId();
                if (runningJobIds.contains(jobId)) {
                    log.info("Killing job " + job.getId());
                    try {
                        jobService.killJob(job.getAppId());
                    } catch (Exception e) {
                        log.warn("Cannot kill job " + jobId, e);
                    }
                }
            }
        }
    }
}
