package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import org.springframework.stereotype.Component;

@Component("amStatsLeafSubstitutionTransformer")
public class AMStatsLeafSubstitutionTransformer extends AbstractStatsDataflowTransformer {

    @Override
    public String getName() {
        return "amStatsLeafSubstitutionTransformer";
    }

    @Override
    protected String getDataFlowBeanName() {
        return "amStatsLeafSubstitutionFlow";
    }
}
