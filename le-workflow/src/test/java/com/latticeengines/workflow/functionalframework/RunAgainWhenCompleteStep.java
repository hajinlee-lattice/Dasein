package com.latticeengines.workflow.functionalframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractStep;
import com.latticeengines.workflow.exposed.build.BaseStepConfiguration;

@Component("runAgainWhenCompleteStep")
public class RunAgainWhenCompleteStep extends AbstractStep<BaseStepConfiguration> {

    private static final Log log = LogFactory.getLog(RunAgainWhenCompleteStep.class);

    @Override
    public void initialize() {
        this.setRunAgainWhenComplete(true);
    }

    @Override
    public void execute() {
        log.info("Inside RunAgainWhenCompleteStep execute()");
    }

}
