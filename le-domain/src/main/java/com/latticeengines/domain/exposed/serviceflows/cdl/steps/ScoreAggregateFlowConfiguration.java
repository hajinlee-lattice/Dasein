package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.common.exposed.util.NamingUtils;

public class ScoreAggregateFlowConfiguration extends BaseCDLDataFlowStepConfiguration {

    public ScoreAggregateFlowConfiguration() {
        setBeanName("scoreAggregate");
        setTargetTableName(NamingUtils.timestamp("ScoreAggregateFlow"));
    }

}
