package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import org.springframework.stereotype.Component;

@Component("amStatsDimAggregateWithHQDunsTransformer")
public class AMStatsDimAggregateWithHQDunsTransformer extends AbstractStatsDataflowTransformer {

    @Override
    public String getName() {
        return "amStatsDimAggregateWithHQDunsTransformer";
    }

    @Override
    protected String getDataFlowBeanName() {
        return "amStatsDimAggregateWithHQDunsFlow";
    }

}
