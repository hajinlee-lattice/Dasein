package com.latticeengines.release.jenkins.activities;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.domain.JenkinsBuildStatus;
import com.latticeengines.release.exposed.domain.JenkinsJobParameters;
import com.latticeengines.release.exposed.domain.JenkinsJobParameters.NameValuePair;
import com.latticeengines.release.exposed.domain.StatusContext;

public class RunJenkinsDeploymentJobActivity extends RunJenkinsJobActivity {

    public RunJenkinsDeploymentJobActivity(String url, ErrorHandler errorHandler) {
        super(url, errorHandler);
    }

    @Override
    public StatusContext runActivity() {
        JenkinsBuildStatus status = jenkinsService.getLastBuildStatus(url);
        if (status.getIsBuilding()) {
            log.warn(String.format("There is already a deployment job %d running", status.getNumber()));
            waitUtilNoJobIsRunning(0L, url);
        }
        log.info("Launching a new deployment job now.");
        JenkinsJobParameters jenkinsParameters = constructReleaseProcessParameters();
        jenkinsService.triggerJenkinsJobWithParameters(url, jenkinsParameters);
        waitUtilNoJobIsRunning(status.getNumber(), url);
        status = jenkinsService.getLastBuildStatus(url);
        String message = String.format("The deployment job %d has completed with result %s", status.getNumber(),
                status.getResult());
        log.info(message);
        statusContext.setResponseMessage(message);
        return statusContext;
    }

    private JenkinsJobParameters constructReleaseProcessParameters() {
        JenkinsJobParameters jenkinsParameters = new JenkinsJobParameters();
        NameValuePair branchName = new NameValuePair("SVN_BRANCH_NAME", "release_" + processContext.getReleaseVersion());
        NameValuePair branchDir = new NameValuePair("SVN_DIR", "tags");
        List<NameValuePair> nameValuePairs = Arrays.asList(new NameValuePair[] { branchName, branchDir });
        jenkinsParameters.setNameValuePairs(nameValuePairs);
        return jenkinsParameters;
    }
}
