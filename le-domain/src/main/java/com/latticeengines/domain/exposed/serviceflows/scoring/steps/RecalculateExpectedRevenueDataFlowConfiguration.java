package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.latticeengines.common.exposed.util.NamingUtils;

public class RecalculateExpectedRevenueDataFlowConfiguration extends BaseScoringDataFlowStepConfiguration {

    public RecalculateExpectedRevenueDataFlowConfiguration() {
        setBeanName("recalculateExpectedRevenue");
        setTargetTableName(NamingUtils.uuid("RecalculateExpectedRevenue"));
    }
}
