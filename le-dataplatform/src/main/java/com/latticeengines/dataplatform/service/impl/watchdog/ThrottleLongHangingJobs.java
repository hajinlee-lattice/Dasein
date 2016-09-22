package com.latticeengines.dataplatform.service.impl.watchdog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.AppInfo;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.yarn.client.ContainerProperty;
import com.latticeengines.domain.exposed.dataplatform.Job;

@Component("throttleLongHangingJobs")
public class ThrottleLongHangingJobs extends WatchdogPlugin {
    private static final Log log = LogFactory.getLog(ThrottleLongHangingJobs.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Value("${dataplatform.throttle.threshold:600000}")
    private long throttleThreshold;

    @Value("${dataplatform.yarn.job.basedir}")
    private String hdfsJobBaseDir;

    private Map<String, AppStatus> appRecords = new HashMap<String, AppStatus>();

    public ThrottleLongHangingJobs() {
        register(this);
    }

    @Override
    public void run(JobExecutionContext context) throws JobExecutionException {
        ArrayList<AppInfo> appInfos = yarnService.getApplications("states=RUNNING").getApps();

        if (appInfos.isEmpty()) {
            log.info("No application is running, going to sleep");
            return;
        }

        log.info("Throttle-hanging-jobs thread is monitoring " + appInfos.size() + " running applications.");
        removeCompletedApps(appInfos);
        List<String> appsToKill = updateAppRecords(appInfos);
        throttle(appsToKill);
    }

    private void removeCompletedApps(ArrayList<AppInfo> appInfos) {
        List<String> runningAppIds = new ArrayList<String>();
        for (AppInfo appInfo : appInfos) {
            runningAppIds.add(appInfo.getAppId());
        }

        // Deletes appIds from HashMap while iterating through it
        Iterator<String> iter = appRecords.keySet().iterator();
        while (iter.hasNext()) {
            String appId = iter.next();
            if (!runningAppIds.contains(appId)) {
                log.debug("Removing completed application with id: " + appId);
                iter.remove();
            }
        }
    }

    private List<String> updateAppRecords(ArrayList<AppInfo> appInfos) {
        List<String> appsToKill = new ArrayList<String>();

        for (AppInfo appInfo : appInfos) {
            String appId = appInfo.getAppId();
            if (appRecords.containsKey(appId)) {
                // Apps in progress
                AppStatus status = appRecords.get(appId);
                if (status.getProgress() != appInfo.getProgress()) {
                    // Update progress
                    status.setProgress(appInfo.getProgress());
                    status.setTimeAtLastProgress(System.currentTimeMillis());
                    appRecords.put(appId, status);
                } else {
                    long elapsedTime = System.currentTimeMillis() - status.getTimeAtLastProgress();
                    if (elapsedTime > throttleThreshold) {
                        log.info("Throttling application: " + appId + " with " + elapsedTime
                                + " elapsed milliseconds since last progress at " + status.getProgress());
                        appsToKill.add(appId);
                    }
                }
            } else {
                // new Apps submitted
                AppStatus status = new AppStatus(System.currentTimeMillis(), appInfo.getProgress());
                appRecords.put(appId, status);
            }
        }

        return appsToKill;
    }

    private void throttle(List<String> appsToKill) {
        if (appsToKill.isEmpty()) {
            return;
        }

        log.info("Throttling " + appsToKill.size() + " applications");
        List<String> appsKilled = new ArrayList<String>();
        for (String appId : appsToKill) {
            try {
                modelingJobService.killJob(ConverterUtils.toApplicationId(appId));
                appsKilled.add(appId);
                appRecords.remove(appId);
            } catch (Exception e) {
                log.warn("Cannot kill job with id : " + appId, e);
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

    }

    private class AppStatus {

        private long timeAtLastProgress;
        private float progress;

        public AppStatus(long timeAtLastProgress, float progress) {
            this.setTimeAtLastProgress(timeAtLastProgress);
            this.setProgress(progress);
        }

        public long getTimeAtLastProgress() {
            return timeAtLastProgress;
        }

        public void setTimeAtLastProgress(long timeAtLastProgress) {
            this.timeAtLastProgress = timeAtLastProgress;
        }

        public float getProgress() {
            return progress;
        }

        public void setProgress(float progress) {
            this.progress = progress;
        }
    }
}
