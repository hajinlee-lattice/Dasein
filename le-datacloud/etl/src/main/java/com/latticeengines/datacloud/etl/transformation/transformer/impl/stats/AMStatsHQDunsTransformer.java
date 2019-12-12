package com.latticeengines.datacloud.etl.transformation.transformer.impl.stats;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.transformation.transformer.impl.AbstractDataflowTransformer;
import com.latticeengines.domain.exposed.datacloud.dataflow.stats.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.stats.AccountMasterStatisticsConfig;

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
