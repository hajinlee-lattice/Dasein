package com.latticeengines.workflow.functionalframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.build.AbstractStep;

@Component("anotherSuccessfulStep")
public class AnotherSuccessfulStep extends AbstractStep {

    private static final Log log = LogFactory.getLog(AnotherSuccessfulStep.class);

    @Override
    public void execute() {
        log.info("Inside AnotherSuccessfulStep execute()");
    }

}
