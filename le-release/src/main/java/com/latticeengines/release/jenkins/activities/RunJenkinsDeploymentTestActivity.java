package com.latticeengines.release.jenkins.activities;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.domain.JenkinsBuildStatus;
import com.latticeengines.release.exposed.domain.StatusContext;

public class RunJenkinsDeploymentTestActivity extends RunJenkinsJobActivity {

    public RunJenkinsDeploymentTestActivity(String url, ErrorHandler errorHandler) {
        super(url, errorHandler);
    }

    @Override
    public StatusContext runActivity() {
        JenkinsBuildStatus status = jenkinsService.getLastBuildStatus(url);
        if (status.getIsBuilding()) {
            log.warn(String.format("There is already a deployment test %d running", status.getNumber()));
            waitUtilNoJobIsRunning(0L, url);
        }
        log.info("Updating deployment test jenkins configuration.");
        jenkinsService.updateSVNBranchName(url, String.format("release_%s", processContext.getReleaseVersion()));
        log.info("Launching a new job with latest configuration now.");
        jenkinsService.triggerJenkinsJobWithOutParameters(url);
        waitUtilNoJobIsRunning(status.getNumber(), url);
        status = jenkinsService.getLastBuildStatus(url);
        String message = String.format("The deployment test %d has completed with result %s", status.getNumber(),
                status.getResult());
        log.info(message);
        statusContext.setResponseMessage(message);
        return statusContext;
    }
}
