package com.latticeengines.workflow.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.build.WorkflowStep;

@Component("modelLoadData")
public class ModelLoadData extends WorkflowStep {

    private static final Log log = LogFactory.getLog(ModelLoadData.class);

    @Override
    public void execute() {
        log.info("Inside ModelLoadData execute()");
    }

}
