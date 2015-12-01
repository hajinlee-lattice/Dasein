package com.latticeengines.workflow.functionalframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("failableStep")
public class FailableStep extends AbstractStep<BaseStepConfiguration> {

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
