package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.step.IterativeStepConfig;

public class TestIterativeStepConfig extends TransformerConfig {
    @JsonProperty("IterateStrategy")
    private IterativeStepConfig.ConvergeOnCount iterateStrategy;

    public IterativeStepConfig.ConvergeOnCount getIterateStrategy() {
        return iterateStrategy;
    }

    public void setIterateStrategy(IterativeStepConfig.ConvergeOnCount iterateStrategy) {
        this.iterateStrategy = iterateStrategy;
    }
}
