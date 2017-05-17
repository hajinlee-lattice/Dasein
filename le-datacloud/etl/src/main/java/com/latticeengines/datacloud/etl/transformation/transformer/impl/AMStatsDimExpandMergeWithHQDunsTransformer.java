package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import org.springframework.stereotype.Component;

@Component("amStatsDimExpandMergeWithHQDunsTransformer")
public class AMStatsDimExpandMergeWithHQDunsTransformer extends AbstractStatsDataflowTransformer {

    @Override
    public String getName() {
        return "amStatsDimExpandMergeWithHQDunsTransformer";
    }

    @Override
    protected String getDataFlowBeanName() {
        return "amStatsDimExpandMergeWithHQDunsFlow";
    }
}
