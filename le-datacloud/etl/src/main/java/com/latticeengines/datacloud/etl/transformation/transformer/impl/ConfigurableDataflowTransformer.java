package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.datacloud.etl.transformation.service.impl.SimpleTransformationDataFlowService;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

public class ConfigurableDataflowTransformer extends AbstractDataflowTransformer<TransformerConfig, TransformationFlowParameters> {

    private static final Log log = LogFactory.getLog(AbstractTransformer.class);

    private String transformerName;

    private String dataFlowBeanName;

    private Class<? extends TransformerConfig> configClass;

    @Override
    public String getName() {
        return transformerName;
    }

    public void setName(String transformerName) {
        this.transformerName = transformerName;
    }

    @Override
    protected String getDataFlowBeanName() {
        return dataFlowBeanName;
    }

    public void setDataFlowBeanName(String dataFlowBeanName) {
        this.dataFlowBeanName = dataFlowBeanName;
    }

    public void setDataFlowService(SimpleTransformationDataFlowService dataFlowService) {
        this.dataFlowService = dataFlowService;
    }

    public void setSourceColumnEntityMgr(SourceColumnEntityMgr sourceColumnEntityMgr) {
        this.sourceColumnEntityMgr = sourceColumnEntityMgr;
    }


    @Override
    protected Class<TransformationFlowParameters> getDataFlowParametersClass() {
        return TransformationFlowParameters.class;
    }


    public void setConfigClass(Class<? extends TransformerConfig> configClass) {
        this.configClass = configClass;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return configClass;
    }

    @Override
    public boolean validateConfig(String confStr, List<String> sourceNames) {

        boolean result = false;

        TransformerConfig config = null;
        try {
            config = JsonUtils.deserialize(confStr, configClass);
        } catch (Exception e) {
            log.error("Failed to convert tranformer config.", e);
        }

        if (config != null) {
            result = config.validate(sourceNames);
        }

        return result;
    }

    @Override
    public boolean validateConfig(TransformerConfig config, List<String> baseSources) {
         return true;
    }
}
