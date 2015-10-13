package com.latticeengines.workflow.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.build.WorkflowStep;

@Component("ModelProfileData")
public class ModelProfileData extends WorkflowStep {

    private static final Log log = LogFactory.getLog(ModelProfileData.class);

    @Override
    public void execute() {
        log.info("Inside ModelProfileData execute()");
    }

}
