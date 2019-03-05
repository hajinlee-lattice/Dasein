package com.latticeengines.dataplatform.service.impl.watchdog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.GetApplicationsRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.yarn.exposed.client.ContainerProperty;

@Component("throttleLongHangingJobs")
public class ThrottleLongHangingJobs extends WatchdogPlugin {
    private static final Logger log = LoggerFactory.getLogger(ThrottleLongHangingJobs.class);

    @Inject
    private Configuration yarnConfiguration;

    @Value("${dataplatform.throttle.threshold:600000}")
    private long throttleThreshold;

    @Value("${dataplatform.yarn.job.basedir}")
    private String hdfsJobBaseDir;

    private Map<ApplicationId, AppStatus> appRecords = new HashMap<>();

    public ThrottleLongHangingJobs() {
        // Disable this job.
        // register(this);
    }

    @Override
    public void run() {
        List<ApplicationReport> appReports = yarnService.getRunningApplications(GetApplicationsRequest.newInstance());

        if (appReports.isEmpty()) {
            log.info("No application is running, going to sleep");
            return;
        }

        log.info("Throttle-hanging-jobs thread is monitoring " + appReports.size() + " running applications.");
        removeCompletedApps(appReports);
        List<ApplicationId> appsToKill = updateAppRecords(appReports);
        throttle(appsToKill);
    }

    private void removeCompletedApps(List<ApplicationReport> appReports) {
        List<ApplicationId> runningAppIds = new ArrayList<>();
        for (ApplicationReport appReport : appReports) {
            runningAppIds.add(appReport.getApplicationId());
        }

        // Deletes appIds from HashMap while iterating through it
        Iterator<ApplicationId> iter = appRecords.keySet().iterator();
        while (iter.hasNext()) {
            ApplicationId appId = iter.next();
            if (!runningAppIds.contains(appId)) {
                log.debug("Removing completed application with id: " + appId);
                iter.remove();
            }
        }
    }

    private List<ApplicationId> updateAppRecords(List<ApplicationReport> appReports) {
        List<ApplicationId> appsToKill = new ArrayList<>();

        for (ApplicationReport appReport : appReports) {
            ApplicationId appId = appReport.getApplicationId();
            if (appRecords.containsKey(appId)) {
                // Apps in progress
                AppStatus status = appRecords.get(appId);
                if (status.getProgress() != appReport.getProgress()) {
                    // Update progress
                    status.setProgress(appReport.getProgress());
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
                AppStatus status = new AppStatus(System.currentTimeMillis(), appReport.getProgress());
                appRecords.put(appId, status);
            }
        }

        return appsToKill;
    }

    private void throttle(List<ApplicationId> appsToKill) {
        if (appsToKill.isEmpty()) {
            return;
        }

        log.info("Throttling " + appsToKill.size() + " applications");
        List<ApplicationId> appsKilled = new ArrayList<>();
        for (ApplicationId appId : appsToKill) {
            try {
                modelingJobService.killJob(appId);
                appsKilled.add(appId);
                appRecords.remove(appId);
            } catch (Exception e) {
                log.warn("Cannot kill job with id : " + appId);
            }
        }

        // clean up job directories
        List<Job> jobsKilled = jobEntityMgr
                .findAllByObjectIds(appsKilled.stream().map(appId -> appId.toString()).collect(Collectors.toList()));
        for (Job job : jobsKilled) {
            String dir = hdfsJobBaseDir + "/" + job.getContainerPropertiesObject().get(ContainerProperty.JOBDIR.name());
            try {
                HdfsUtils.rmdir(yarnConfiguration, dir);
            } catch (Exception e) {
                log.warn("Could not delete job dir " + dir + " due to exception:\n" + e.getMessage());
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
