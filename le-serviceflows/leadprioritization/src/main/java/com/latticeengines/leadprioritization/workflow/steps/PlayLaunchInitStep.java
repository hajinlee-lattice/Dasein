package com.latticeengines.leadprioritization.workflow.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("playLaunchInitStep")
public class PlayLaunchInitStep extends BaseWorkflowStep<PlayLaunchInitStepConfiguration> {

    private static final Log log = LogFactory.getLog(PlayLaunchInitStep.class);

    @Override
    public void execute() {
        log.info("Inside PlayLaunchInitStep execute()");
        PlayLaunchInitStepConfiguration config = getConfiguration();
        log.info("For tenant: " + config.getCustomerSpace().toString());
    }
}
