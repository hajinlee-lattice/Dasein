package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.common.exposed.util.NamingUtils;

public class ScoreAggregateFlowConfiguration extends BaseCDLDataFlowStepConfiguration {

    private Boolean expectedValue;

    public ScoreAggregateFlowConfiguration() {
        setBeanName("scoreAggregate");
        setTargetTableName(NamingUtils.timestampWithRandom("ScoreAggregateFlow"));
    }

    public Boolean getExpectedValue() {
        return expectedValue;
    }

    public void setExpectedValue(Boolean expectedValue) {
        this.expectedValue = expectedValue;
    }

}
