package com.latticeengines.common.exposed.util;

import java.io.IOException;
import java.util.EnumSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

import com.google.common.base.Strings;

public class YarnUtils {

    private static Log log = LogFactory.getLog(YarnUtils.class);

    public static final EnumSet<FinalApplicationStatus> TERMINAL_STATUS = EnumSet.of(FinalApplicationStatus.FAILED,
            FinalApplicationStatus.KILLED, FinalApplicationStatus.SUCCEEDED);

    public static final EnumSet<YarnApplicationState> TERMINAL_APP_STATE = EnumSet.of(YarnApplicationState.FAILED,
            YarnApplicationState.KILLED, YarnApplicationState.FINISHED);

    public static boolean isPrempted(String diagnostics) {
        if (Strings.isNullOrEmpty(diagnostics))
            return false;

        return (diagnostics.contains("-102") && diagnostics.contains("Container preempted by scheduler"));
    }

    public static ApplicationReport getApplicationReport(Configuration yarnConfiguration, ApplicationId applicationId)
            throws YarnException, IOException {
        String rmAddr = yarnConfiguration.get("dataplatform.yarn.resourcemanager.address");
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
        ApplicationReport report;
        try {
            report = yarnClient.getApplicationReport(applicationId);
        } finally {
            yarnClient.stop();
        }
        return report;
    }

    public static FinalApplicationStatus waitFinalStatusForAppId(Configuration yarnConfiguration,
            ApplicationId applicationId) {
        return waitFinalStatusForAppId(yarnConfiguration, applicationId, 3600);
    }

    public static FinalApplicationStatus waitFinalStatusForAppId(Configuration yarnConfiguration,
            ApplicationId applicationId, Integer timeoutInSec) {
        log.info("Wait " + applicationId + " for " + timeoutInSec + " seconds.");
        FinalApplicationStatus finalStatus = null;
        Long startTime = System.currentTimeMillis();
        int maxTries = 10000;
        int i = 0;
        do {
            try {
                ApplicationReport report = getApplicationReport(yarnConfiguration, applicationId);
                finalStatus = report.getFinalApplicationStatus();
                String logMsg = "Waiting for application [" + applicationId + "]: " + report.getYarnApplicationState();
                if (report.getYarnApplicationState().equals(YarnApplicationState.RUNNING)) {
                    logMsg += " " + report.getProgress() * 100 + " %";
                }
                log.info(logMsg);
            } catch (Exception e) {
                log.warn("Failed to get application status of application id " + applicationId);
            }
            try {
                Thread.sleep(5000L);
            } catch (InterruptedException e) {
                // Do nothing for InterruptedException
            }
            i++;

            if (i >= maxTries || (System.currentTimeMillis() - startTime) >= timeoutInSec * 1000L) {
                break;
            }
        } while (!YarnUtils.TERMINAL_STATUS.contains(finalStatus));

        log.info("The terminal status of application [" + applicationId + "] is " + finalStatus);

        return finalStatus;

    }

}