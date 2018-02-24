package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.latticeengines.common.exposed.util.NamingUtils;

public class ComputeLiftDataFlowConfiguration extends BaseScoringDataFlowStepConfiguration {

    public ComputeLiftDataFlowConfiguration() {
        setBeanName("computeLift");
        setTargetTableName(NamingUtils.uuid("ComputeLift"));
    }

}
