package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.latticeengines.common.exposed.util.NamingUtils;

public class RecalculatePercentileScoreDataFlowConfiguration extends BaseScoringDataFlowStepConfiguration {

    public RecalculatePercentileScoreDataFlowConfiguration() {
        setBeanName("recalculatePercentileScore");
        setTargetTableName(NamingUtils.uuid("RecalculatePercentileScore"));
    }
}
