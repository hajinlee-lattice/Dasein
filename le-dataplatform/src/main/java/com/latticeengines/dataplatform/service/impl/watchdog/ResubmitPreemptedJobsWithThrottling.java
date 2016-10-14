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
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;

@Component("resubmitPreemptedJobsWithThrottling")
public class ResubmitPreemptedJobsWithThrottling extends WatchdogPlugin {
    private static final Log log = LogFactory.getLog(ResubmitPreemptedJobsWithThrottling.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${dataplatform.yarn.job.basedir}")
    private String hdfsJobBaseDir;

    public ResubmitPreemptedJobsWithThrottling() {
        // Disable for now
        // register(this);
    }

    @Override
    public void run(JobExecutionContext context) throws JobExecutionException {
        ThrottleConfiguration latestConfig = throttleConfigurationEntityMgr.getLatestConfig();
        List<ModelingJob> jobsToKill = getJobsToKill(latestConfig);
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

    private int resubmitPreemptedJobs(List<ModelingJob> jobsToExcludeFromResubmission) {
        Set<String> jobIdsToExcludeFromResubmission = new HashSet<String>();
        for (ModelingJob modelingJob : jobsToExcludeFromResubmission) {
            jobIdsToExcludeFromResubmission.add(modelingJob.getId());
        }
        List<String> appIds = new ArrayList<String>();
        for (AppInfo appInfo : yarnService.getPreemptedApps()) {
            String appId = appInfo.getAppId();
            // if P0, resubmit immediately with no delay. If any other
            // priorities, delay by some latency
            if (!jobIdsToExcludeFromResubmission.contains(appId)
                    && (appInfo.getQueue().contains("Priority0") || System.currentTimeMillis()
                            - appInfo.getFinishTime() > retryWaitTime)//
                    && System.currentTimeMillis() - appInfo.getFinishTime() < maxRetryTimeThreshold) {
                appIds.add(appId);
            }
        }

        List<Job> jobsToResubmit = jobEntityMgr.findAllByObjectIds(appIds);
        for (Job modelingJob : jobsToResubmit) {
            modelingJobService.resubmitPreemptedJob((ModelingJob) modelingJob);
        }
        return jobsToResubmit.size();
    }

    private List<ModelingJob> getJobsToKill(ThrottleConfiguration config) {
        List<ModelingJob> jobsToKill = new ArrayList<>();

        if (config != null) {
            int cutoffIndex = config.getJobRankCutoff();
            List<Job> runningJobs = jobEntityMgr.findAllByObjectIds(getRunningJobIds());
            Map<Long, Integer> modelToJobCounter = new HashMap<>();

            for (Job modelingJob : runningJobs) {
                Long modelId = ((ModelingJob) modelingJob).getModel().getPid();
                Integer jobCounter = modelToJobCounter.get(modelId);
                if (jobCounter == null) {
                    jobCounter = 0;
                }
                jobCounter += 1;
                modelToJobCounter.put(modelId, jobCounter);
                if (jobCounter >= cutoffIndex) {
                    if (log.isDebugEnabled()) {
                        log.debug("Finding job [over the rank cutoff]: " + modelingJob.getId() + " for model "
                                + modelId);
                    }
                    jobsToKill.add(((ModelingJob) modelingJob));
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
    private int throttle(ThrottleConfiguration config, List<ModelingJob> jobs) {
        Set<String> runningJobIds = new HashSet<String>(getRunningJobIds());

        List<String> appsKilled = new ArrayList<String>();

        if (config != null && config.isEnabled() && config.isImmediate()) {
            for (ModelingJob modelingJob : jobs) {
                String jobId = modelingJob.getId();
                if (runningJobIds.contains(jobId)) {
                    log.info("Killing job " + jobId);
                    try {
                        modelingJobService.killJob(modelingJob.getAppId());
                        appsKilled.add(jobId);
                    } catch (Exception e) {
                        log.warn("Cannot kill job " + jobId, e);
                    }
                }
            }
        }

        // clean up job directories
        List<Job> jobsKilled = jobEntityMgr.findAllByObjectIds(appsKilled);
        for (Job modelingJob : jobsKilled) {
            String dir = hdfsJobBaseDir + "/"
                    + modelingJob.getContainerPropertiesObject().get(ContainerProperty.JOBDIR.name());
            try {
                HdfsUtils.rmdir(yarnConfiguration, dir);
            } catch (Exception e) {
                log.warn("Could not delete job dir " + dir + " due to exception:\n" + ExceptionUtils.getStackTrace(e));
            }
        }
        return appsKilled.size();
    }
}
