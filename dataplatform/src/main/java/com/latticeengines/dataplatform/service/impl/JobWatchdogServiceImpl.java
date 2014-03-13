package com.latticeengines.dataplatform.service.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppsInfo;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.dataplatform.entitymanager.JobEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.exposed.domain.Job;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;
import com.latticeengines.dataplatform.exposed.service.YarnService;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.dataplatform.service.JobWatchdogService;


public class JobWatchdogServiceImpl extends QuartzJobBean implements JobWatchdogService {
    
    private JobService jobService;
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;
    private ModelEntityMgr modelEntityMgr;
    private YarnService yarnService;
    private JobEntityMgr jobEntityMgr;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        run(context);
    }
    
    @Override
    public void run(JobExecutionContext context) throws JobExecutionException {
        ThrottleConfiguration latestConfig = throttleConfigurationEntityMgr.getLatestConfig();
        List<Job> jobsToKill = getJobsToKill(latestConfig);
        //resubmitPreemptedJobs(jobsToKill);
        throttle(latestConfig, jobsToKill);
    }
    
    private void resubmitPreemptedJobs(List<Job> jobsToExcludeFromResubmission) {
        Set<String> jobIdsToExcludeFromResubmission = new HashSet<String>();
        for (Job job : jobsToExcludeFromResubmission) {
            jobIdsToExcludeFromResubmission.add(job.getId());
        }
        AppsInfo appsInfo = yarnService.getApplications("states=FAILED");
        ArrayList<AppInfo> appInfos = appsInfo.getApps();
        Collections.sort(appInfos, new Comparator<AppInfo>() {

            @Override
            public int compare(AppInfo o1, AppInfo o2) {
                if (o1.getStartTime() - o2.getStartTime() <= 0) {
                    return -1;
                }
                return 1;
            }
            
        });
        Set<String> appIds = new HashSet<String>();
        for (AppInfo appInfo : appInfos) {
            String diagnostics = appInfo.getNote();
            String appId = appInfo.getAppId();
            if (diagnostics.contains("-102") && !jobIdsToExcludeFromResubmission.contains(appId)) {
                appIds.add(appId);
            }
        }
        Set<Job> jobsToResubmit = jobEntityMgr.getByIds(appIds);
        for (Job job : jobsToResubmit) {
            jobService.submitJob(job);
        }
    }
    
    private List<Job> getJobsToKill(ThrottleConfiguration config) {
        int cutoffIndex = config.getJobRankCutoff();
        List<Job> jobs = new ArrayList<Job>();
        List<Model> models = modelEntityMgr.getAll();
        for (Model model : models) {
            List<Job> ownedJobs = model.getJobs();
            for (int i = 1; i <= ownedJobs.size(); i++) {
                if (i >= cutoffIndex) {
                    jobs.add(ownedJobs.get(i - 1));
                }
            }
            
        }
        return jobs;
    }
    
    private void throttle(ThrottleConfiguration config, List<Job> jobs) {
        if (config.isEnabled() && config.isImmediate()) {
            for (Job job : jobs) {
                jobService.killJob(job.getAppId());
            }
        }
    }

    public JobService getJobService() {
        return jobService;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

    public ThrottleConfigurationEntityMgr getThrottleConfigurationEntityMgr() {
        return throttleConfigurationEntityMgr;
    }

    public void setThrottleConfigurationEntityMgr(ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr) {
        this.throttleConfigurationEntityMgr = throttleConfigurationEntityMgr;
    }

    public ModelEntityMgr getModelEntityMgr() {
        return modelEntityMgr;
    }

    public void setModelEntityMgr(ModelEntityMgr modelEntityMgr) {
        this.modelEntityMgr = modelEntityMgr;
    }

    public YarnService getYarnService() {
        return yarnService;
    }

    public void setYarnService(YarnService yarnService) {
        this.yarnService = yarnService;
    }

    public JobEntityMgr getJobEntityMgr() {
        return jobEntityMgr;
    }

    public void setJobEntityMgr(JobEntityMgr jobEntityMgr) {
        this.jobEntityMgr = jobEntityMgr;
    }

}
