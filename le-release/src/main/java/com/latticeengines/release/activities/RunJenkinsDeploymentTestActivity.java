package com.latticeengines.release.activities;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import com.latticeengines.release.error.handler.ErrorHandler;
import com.latticeengines.release.exposed.domain.JenkinsBuildStatus;
import com.latticeengines.release.exposed.domain.ProcessContext;

@Component("runJenkinsDeploymentTestActivity")
public class RunJenkinsDeploymentTestActivity extends RunJenkinsJobActivity {

    @Autowired
    public RunJenkinsDeploymentTestActivity(@Qualifier("defaultErrorHandler") ErrorHandler errorHandler) {
        super(errorHandler);
    }

    @Override
    public ProcessContext runActivity(ProcessContext context) {
        String url = context.getUrl();
        JenkinsBuildStatus status = jenkinsService.getLastBuildStatus(url);
        if(status.getIsBuilding() == true){
            log.warn(String.format("There is already a deployment test %d running", status.getNumber()));
            waitUtilNoJobIsRunning(url);
        }
        log.info("Launching a new job now.");
        jenkinsService.triggerJenkinsJobWithOutParameters(url);
        waitUtilNoJobIsRunning(url);
        status = jenkinsService.getLastBuildStatus(url);
        String message = String.format("The deployment test %d has completed with result %s", status.getNumber(), status.getResult());
        log.info(message);
        context.setResponseMessage(message);
        return context;
    }
}
