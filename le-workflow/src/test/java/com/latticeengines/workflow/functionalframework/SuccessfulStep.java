package com.latticeengines.workflow.functionalframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.build.AbstractStep;

@Component("SuccessfulStep")
public class SuccessfulStep extends AbstractStep {

    private static final Log log = LogFactory.getLog(SuccessfulStep.class);

    @Override
    public void execute() {
        log.info("Inside SuccessfulStep execute()");
    }

}
