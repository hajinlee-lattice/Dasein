package com.latticeengines.release.jenkins.activities;

import java.util.Arrays;
import java.util.List;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.domain.JenkinsBuildStatus;
import com.latticeengines.release.exposed.domain.JenkinsJobParameters;
import com.latticeengines.release.exposed.domain.ProcessContext;
import com.latticeengines.release.exposed.domain.JenkinsJobParameters.NameValuePair;

public class RunJenkinsDeploymentJobActivity extends RunJenkinsJobActivity {

    public RunJenkinsDeploymentJobActivity(String url, ErrorHandler errorHandler) {
        super(url, errorHandler);
    }

    @Override
    public ProcessContext runActivity(ProcessContext context) {
        JenkinsBuildStatus status = jenkinsService.getLastBuildStatus(url);
        if (status.getIsBuilding()) {
            log.warn(String.format("There is already a deployment job %d running", status.getNumber()));
            waitUtilNoJobIsRunning(0L, url);
        }
        log.info("Launching a new deployment job now.");
        JenkinsJobParameters jenkinsParameters = constructReleaseProcessParameters(context);
        jenkinsService.triggerJenkinsJobWithParameters(url, jenkinsParameters);
        waitUtilNoJobIsRunning(status.getNumber(), url);
        status = jenkinsService.getLastBuildStatus(url);
        String message = String.format("The deployment job %d has completed with result %s", status.getNumber(),
                status.getResult());
        log.info(message);
        context.setResponseMessage(message);
        return context;
    }

    private JenkinsJobParameters constructReleaseProcessParameters(ProcessContext context) {
        JenkinsJobParameters jenkinsParameters = new JenkinsJobParameters();
        NameValuePair branchName = new NameValuePair("SVN_BRANCH_NAME", "release_" + context.getReleaseVersion());

        List<NameValuePair> nameValuePairs = Arrays.asList(new NameValuePair[] { branchName });
        jenkinsParameters.setNameValuePairs(nameValuePairs);
        return jenkinsParameters;
    }
}
