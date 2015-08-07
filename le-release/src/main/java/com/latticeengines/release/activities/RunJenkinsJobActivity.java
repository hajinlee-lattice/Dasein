package com.latticeengines.release.activities;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.activities.BaseActivity;
import com.latticeengines.release.exposed.domain.JenkinsBuildStatus;
import com.latticeengines.release.jenkins.service.JenkinsService;

public abstract class RunJenkinsJobActivity extends BaseActivity {

    public RunJenkinsJobActivity(ErrorHandler errorHandler) {
        super(errorHandler);
    }

    @Autowired
    protected JenkinsService jenkinsService;

    protected void waitUtilNoJobIsRunning(String url) {
        while(true){
            try{
                Thread.sleep(45000L);
                JenkinsBuildStatus status = jenkinsService.getLastBuildStatus(url);
                if(status.getIsBuilding() == false){
                    return;
                }
                log.warn(String.format("Waiting for deployment test %d to complete", status.getNumber()));
            }catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
