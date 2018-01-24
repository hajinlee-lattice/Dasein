package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import java.util.UUID;

public class ScoreAggregateFlowConfiguration extends BaseCDLDataFlowStepConfiguration {

    public ScoreAggregateFlowConfiguration() {
        setBeanName("scoreAggregate");
        setTargetTableName("ScoreAggregateFlow_" + UUID.randomUUID().toString());
    }

}
