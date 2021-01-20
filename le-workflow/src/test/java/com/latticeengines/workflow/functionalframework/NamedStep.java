package com.latticeengines.workflow.functionalframework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.workflow.exposed.build.AbstractStep;

public class NamedStep extends AbstractStep<BaseStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(NamedStep.class);

    private final String stepName;
    private final boolean skipOnMissingConfig;

    NamedStep(String stepName) {
        this(stepName, false);
    }

    NamedStep(String stepName, boolean skipOnMissingConfig) {
        this.stepName = stepName;
        this.skipOnMissingConfig = skipOnMissingConfig;
    }

    @Override
    public void execute() {
        log.info(String.format("Inside %s execute()", stepName));
    }

    @Override
    public boolean skipOnMissingConfiguration() {
        return skipOnMissingConfig;
    }

    public String getStepName() {
        return stepName;
    }
}
