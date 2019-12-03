package com.latticeengines.datacloud.etl.transformation.transformer.impl.stats;

import org.springframework.stereotype.Component;

@Component("amStatsLeafNodeTransformer")
public class AMStatsLeafNodeTransformer extends AbstractStatsDataflowTransformer {

    @Override
    public String getName() {
        return "amStatsLeafNodeTransformer";
    }

    @Override
    protected String getDataFlowBeanName() {
        return "amStatsLeafNodeFlow";
    }
}
