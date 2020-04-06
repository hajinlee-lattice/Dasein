package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;

/**
 * Collapse transformer config in to spark job config
 *
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

    @Override
    protected void preSparkJobProcessing(TransformStep step, String workflowDir, S sparkJobConfig,
            TransformerConfig stepConfig) {
        preSparkJobProcessing(step, workflowDir, sparkJobConfig);
    }

    @Override
    protected void postSparkJobProcessing(TransformStep step, String workflowDir, S sparkJobConfig,
            TransformerConfig stepConfig, SparkJobResult sparkJobResult) {
        postSparkJobProcessing(step, workflowDir, sparkJobConfig, sparkJobResult);
    }

    protected void preSparkJobProcessing(TransformStep step, String workflowDir, S sparkJobConfig) {
    }

    protected void postSparkJobProcessing(TransformStep step, String workflowDir, S sparkJobConfig,
            SparkJobResult sparkJobResult) {
    }

}
