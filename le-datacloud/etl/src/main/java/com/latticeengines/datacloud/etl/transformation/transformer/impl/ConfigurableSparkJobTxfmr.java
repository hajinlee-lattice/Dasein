package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

/**
 * Collapse transformer config in to spark job config
 * @param <S>
 */
public abstract class ConfigurableSparkJobTxfmr<S extends SparkJobConfig> //
        extends AbstractSparkTxfmr<S, TransformerConfig> {

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return TransformerConfig.class;
    }

    protected abstract Class<S> getJobConfigClz();

    @Override
    protected S getSparkJobConfig(String configStr) {
        return JsonUtils.deserialize(configStr, getJobConfigClz());
    }

}
