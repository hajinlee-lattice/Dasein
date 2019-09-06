package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;

public class ScoreAggregateFlowConfiguration extends SparkJobStepConfiguration {

    private Boolean expectedValue;

    public ScoreAggregateFlowConfiguration() {
    }

    public Boolean getExpectedValue() {
        return expectedValue;
    }

    public void setExpectedValue(Boolean expectedValue) {
        this.expectedValue = expectedValue;
    }

}
