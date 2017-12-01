package com.latticeengines.workflow.functionalframework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

public class NamedStep extends AbstractStep<BaseStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(NamedStep.class);

    private final String stepName;

    NamedStep(String stepName) {
        this.stepName = stepName;
    }

    @Override
    public void execute() {
        log.info(String.format("Inside %s execute()", stepName));
    }

    public String getStepName() {
        return stepName;
    }
}
