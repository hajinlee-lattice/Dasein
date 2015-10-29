package com.latticeengines.release.jenkins.activities;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.activities.BaseActivity;
import com.latticeengines.release.exposed.domain.JenkinsBuildStatus;
import com.latticeengines.release.jenkins.service.JenkinsService;

public abstract class RunJenkinsJobActivity extends BaseActivity {

    @Autowired
    protected JenkinsService jenkinsService;

    protected String url;

    public RunJenkinsJobActivity(ErrorHandler errorHandler) {
        super(errorHandler);
    }

    public RunJenkinsJobActivity(String url, ErrorHandler errorHandler) {
        this(errorHandler);
        this.url = url;
    }

    protected void waitUtilNoJobIsRunning(long lastBuildNumber, String url) {
        while (true) {
            try {
                Thread.sleep(45000L);
                JenkinsBuildStatus status = jenkinsService.getLastBuildStatus(url);
                if (status.getIsBuilding() == false && status.getNumber() > lastBuildNumber ) {
                    return;
                }
                log.warn(String.format("Waiting for jenkins job %d to complete", status.getNumber()));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
