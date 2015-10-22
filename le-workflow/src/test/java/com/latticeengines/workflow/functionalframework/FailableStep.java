package com.latticeengines.workflow.functionalframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.build.AbstractStep;

@Component("FailableStep")
public class FailableStep extends AbstractStep {

    private static final Log log = LogFactory.getLog(FailableStep.class);

    private boolean fail = true;

    @Override
    public void execute() {
        log.info("Inside FailableStep execute()");
        if (fail) {
            throw new RuntimeException("Simulate runtime failure");
        }
    }

    public boolean isFail() {
        return fail;
    }

    public void setFail(boolean fail) {
        this.fail = fail;
    }

}
