package com.latticeengines.datacloud.etl.transformation.transformer.impl.stats;

import org.springframework.stereotype.Component;

@Component("amStatsMinMaxTransformer")
public class AMStatsMinMaxTransformer extends AbstractStatsDataflowTransformer {

    @Override
    public String getName() {
        return "amStatsMinMaxTransformer";
    }

    @Override
    protected String getDataFlowBeanName() {
        return "amStatsMinMaxFlow";
    }
}
