package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import org.springframework.stereotype.Component;

@Component("amStatsDimExpandMergeTransformer")
public class AMStatsDimExpandMergeTransformer extends AbstractStatsDataflowTransformer {

    @Override
    public String getName() {
        return "amStatsDimExpandMergeTransformer";
    }

    @Override
    protected String getDataFlowBeanName() {
        return "amStatsDimExpandMergeFlow";
    }
}
