package com.latticeengines.release.jenkins.activities;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.domain.JenkinsBuildStatus;
import com.latticeengines.release.exposed.domain.JenkinsJobParameters;
import com.latticeengines.release.exposed.domain.JenkinsJobParameters.NameValuePair;
import com.latticeengines.release.exposed.domain.StatusContext;

@Component("runReleaseProcessActivity")
public class RunReleaseProcessActivity extends RunJenkinsJobActivity {

    @Autowired
    public RunReleaseProcessActivity(@Value("${release.jenkins.release.url}") String url, @Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        super(url, errorHandler);
    }

    @Override
    public StatusContext runActivity() {
        JenkinsJobParameters jenkinsParameters = constructReleaseProcessParameters();
        JenkinsBuildStatus status = jenkinsService.getLastBuildStatus(url);
        jenkinsService.triggerJenkinsJobWithParameters(url, jenkinsParameters);
        waitUtilNoJobIsRunning(status.getNumber(), url);
        status = jenkinsService.getLastBuildStatus(url);
        String message = String.format("The release process %d has completed with result %s", status.getNumber(),
                status.getResult());
        log.info(message);
        statusContext.setResponseMessage(message);
        return statusContext;
    }

    private JenkinsJobParameters constructReleaseProcessParameters() {
        JenkinsJobParameters jenkinsParameters = new JenkinsJobParameters();
        NameValuePair branchName = new NameValuePair("Branch_Name", "develop");
        NameValuePair copyBranchName = new NameValuePair("Copy_Branch_Name", "develop_copy");
        NameValuePair releaseVersion = new NameValuePair("Release_Version", processContext.getReleaseVersion());
        NameValuePair nextReleaseVersion = new NameValuePair("Next_Version_Number", processContext.getNextReleaseVersion());
        NameValuePair product = new NameValuePair("Product", processContext.getProduct());
        NameValuePair svnRevision = new NameValuePair("SVN_REVISION", processContext.getRevision());
        List<NameValuePair> nameValuePairs = Arrays.asList(new NameValuePair[] { branchName, copyBranchName,
                releaseVersion, nextReleaseVersion, product, svnRevision });

        jenkinsParameters.setNameValuePairs(nameValuePairs);
        return jenkinsParameters;
    }
}
