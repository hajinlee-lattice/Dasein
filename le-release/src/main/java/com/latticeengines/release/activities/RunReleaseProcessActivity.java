package com.latticeengines.release.activities;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.domain.JenkinsBuildStatus;
import com.latticeengines.release.exposed.domain.ReleaseProcessParameters;
import com.latticeengines.release.exposed.domain.ReleaseProcessParameters.NameValuePair;
import com.latticeengines.release.exposed.domain.ProcessContext;

@Component("runReleaseProcessActivity")
public class RunReleaseProcessActivity extends RunJenkinsJobActivity {

    @Autowired
    public RunReleaseProcessActivity(@Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        super(errorHandler);
    }

    @Override
    public ProcessContext runActivity(ProcessContext context) {
        ReleaseProcessParameters jenkinsParameters = constructReleaseProcessParameters(context);
        jenkinsService.triggerJenkinsJobWithParameters(context.getUrl(), jenkinsParameters);
        waitUtilNoJobIsRunning(context.getUrl());
        JenkinsBuildStatus status = jenkinsService.getLastBuildStatus(context.getUrl());
        String message = String.format("The release process %d has completed with result %s", status.getNumber(),
                status.getResult());
        log.info(message);
        context.setResponseMessage(message);
        return context;
    }

    private ReleaseProcessParameters constructReleaseProcessParameters(ProcessContext context) {
        ReleaseProcessParameters jenkinsParameters = new ReleaseProcessParameters();
        NameValuePair branchName = new NameValuePair("Branch_Name", "develop");
        NameValuePair copyBranchName = new NameValuePair("Copy_Branch_Name", "develop_copy");
        NameValuePair releaseVersion = new NameValuePair("Release_Version", context.getReleaseVersion());
        NameValuePair nextReleaseVersion = new NameValuePair("Next_Version_Number", context.getNextReleaseVersion());
        NameValuePair product = new NameValuePair("Product", context.getProduct());
        NameValuePair svnRevision = new NameValuePair("SVN_REVISION", context.getRevision());
        List<NameValuePair> nameValuePairs = Arrays.asList(new NameValuePair[] { branchName, copyBranchName,
                releaseVersion, nextReleaseVersion, product, svnRevision });

        jenkinsParameters.setNameValuePairs(nameValuePairs);
        return jenkinsParameters;
    }
}
