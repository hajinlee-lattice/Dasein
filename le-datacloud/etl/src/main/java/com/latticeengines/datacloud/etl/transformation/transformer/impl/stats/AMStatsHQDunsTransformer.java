package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AccountMasterStatisticsConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component("amStatsHQDunsTransformer")
public class AMStatsHQDunsTransformer
        extends AbstractDataflowTransformer<AccountMasterStatisticsConfig, AccountMasterStatsParameters> {

    @Override
    public String getName() {
        return "amStatsHQDunsTransformer";
    }

    @Override
    protected String getDataFlowBeanName() {
        return "amStatsHQDunsFlow";
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return AccountMasterStatisticsConfig.class;
    }

    @Override
    protected Class<AccountMasterStatsParameters> getDataFlowParametersClass() {
        return AccountMasterStatsParameters.class;
    }
}
