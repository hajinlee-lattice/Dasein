package com.latticeengines.datacloud.etl.transformation.transformer.impl.stats;

import org.springframework.stereotype.Component;

@Component("amStatsReportTransformer")
public class AMStatsReportTransformer extends AbstractStatsDataflowTransformer {

    @Override
    public String getName() {
        return "amStatsReportTransformer";
    }

    @Override
    protected String getDataFlowBeanName() {
        return "amStatsReportFlow";
    }
}
