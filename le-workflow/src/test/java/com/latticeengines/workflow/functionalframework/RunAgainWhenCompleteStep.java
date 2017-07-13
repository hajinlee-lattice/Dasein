package com.latticeengines.workflow.functionalframework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

@Component("runAgainWhenCompleteStep")
public class RunAgainWhenCompleteStep extends AbstractStep<BaseStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(RunAgainWhenCompleteStep.class);

    @Override
    public void initialize() {
        this.setRunAgainWhenComplete(true);
    }

    @Override
    public void execute() {
        log.info("Inside RunAgainWhenCompleteStep execute()");
    }

}
