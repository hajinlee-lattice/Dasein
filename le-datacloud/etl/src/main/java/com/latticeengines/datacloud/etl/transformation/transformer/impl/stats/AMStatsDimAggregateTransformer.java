package com.latticeengines.datacloud.etl.transformation.transformer.impl.stats;

import org.springframework.stereotype.Component;

@Component("amStatsDimAggregateTransformer")
public class AMStatsDimAggregateTransformer extends AbstractStatsDataflowTransformer {

    @Override
    public String getName() {
        return "amStatsDimAggregateTransformer";
    }

    @Override
    protected String getDataFlowBeanName() {
        return "amStatsDimAggregateFlow";
    }

}
