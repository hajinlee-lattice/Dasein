package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.etl.transformation.ProgressHelper;
import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.datacloud.etl.transformation.transformer.Transformer;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

public abstract class AbstractTransformer<T extends TransformerConfig> implements Transformer {

    private static final Logger log = LoggerFactory.getLogger(AbstractTransformer.class);

    @Autowired
    protected TransformationProgressEntityMgr progressEntityMgr;

    @Autowired
    private ProgressHelper progressHelper;

    abstract public String getName();

    abstract protected boolean validateConfig(T config, List<String> sourceNames);

    abstract protected boolean transformInternal(TransformationProgress progress, String workflowDir,
            TransformStep step);

    protected Logger getLogger() {
        return log;
    }

    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return TransformerConfig.class;
    }

    @Override
    public boolean validateConfig(String confStr, List<String> sourceNames) {
        if (getConfigurationClass() == TransformerConfig.class) {
            return true;
        } else {
            T config = getConfiguration(confStr);
            if (config == null) {
                return false;
            } else {
                return validateConfig(config, sourceNames);
            }
        }
    }

    @Override
    public void initBaseSources(String confStr, List<String> sourceNames) {
        T config = getConfiguration(confStr);
        if (config == null) {
            return;
        } else {
            initBaseSources(config, sourceNames);
        }
    }

    protected void initBaseSources(T config, List<String> sourceNames) {

    }

    @SuppressWarnings("unchecked")
    protected T getConfiguration(String confStr) {
        T configuration = null;
        Class<? extends TransformerConfig> configClass = getConfigurationClass();
        if (confStr == null) {
            if (configClass == TransformerConfig.class) {
                try {
                    configuration = (T) configClass.newInstance();
                } catch (Exception e) {
                }
            }
        } else {
            try {
                configuration = (T) JsonUtils.deserialize(confStr, configClass);
            } catch (Exception e) {
                log.error("Failed to convert tranformer config.", e);
            }
        }
        return configuration;
    }

    @Override
    public boolean transform(TransformationProgress pipelineProgress, String workflowDir, TransformStep step) {
        try {
            log.info("Start in transformer");
            T configuration = getConfiguration(step.getConfig());
            if (configuration == null) {
                log.error("Invalid transformer configuration");
                updateStatusToFailed(pipelineProgress, "Failed to transform data.", null);
                return false;
            }
            return transformInternal(pipelineProgress, workflowDir, step);
        } catch (Exception e) {
            log.error("Transformer failed to transform", e);
            updateStatusToFailed(pipelineProgress, "Failed to transform data.", e);
            return false;
        }
    }

    protected void updateStatusToFailed(TransformationProgress progress, String errorMsg, Exception e) {
        progressHelper.updateStatusToFailed(progressEntityMgr, progress, errorMsg, e, getLogger());
    }

}
