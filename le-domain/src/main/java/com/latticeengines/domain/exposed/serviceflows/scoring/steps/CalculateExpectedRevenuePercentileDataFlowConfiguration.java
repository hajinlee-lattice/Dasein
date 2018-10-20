package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.latticeengines.common.exposed.util.NamingUtils;

public class CalculateExpectedRevenuePercentileDataFlowConfiguration
        extends BaseScoringDataFlowStepConfiguration {

    public CalculateExpectedRevenuePercentileDataFlowConfiguration() {
        setBeanName("calculateExpectedRevenuePercentile");
        setTargetTableName(NamingUtils.uuid("calculateExpectedRevenuePercentile"));
    }
}
