package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.etl.transformation.ProgressHelper;
import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.datacloud.etl.transformation.transformer.Transformer;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

public abstract class AbstractTransformer<T extends TransformerConfig> implements Transformer {

    private static final Log log = LogFactory.getLog(AbstractTransformer.class);


    @Autowired
    protected TransformationProgressEntityMgr progressEntityMgr;

    @Autowired
    private ProgressHelper progressHelper;

    abstract public String getName();


    abstract protected boolean validateConfig(T config, List<String> sourceNames);

    abstract protected boolean transform(TransformationProgress progress, String workflowDir, Source[] baseSources, List<String> baseVersions,
                             Source[] baseTemplates, Source targetTemplate, T configuration, String confStr);

    protected Log getLogger() {
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

    @SuppressWarnings("unchecked")
    protected T getConfiguration(String confStr) {
        T configuration = null;
        Class<? extends TransformerConfig> configClass = getConfigurationClass();
        if (confStr == null) {
            if (configClass == TransformerConfig.class) {
                try {
                    configuration = (T)configClass.newInstance();
                } catch (Exception e) {
                }
            }
        } else {
            try {
                configuration = (T)JsonUtils.deserialize(confStr, configClass);
            } catch (Exception e) {
                log.error("Failed to convert tranformer config.", e);
            }
        }
        return configuration;
    }

    @Override
    public boolean transform(TransformationProgress progress, String workflowDir, Source[] baseSources, List<String> baseVersions,
                             Source[] baseTemplates, Source targetTemplate, String confStr) {
        try {
            log.info("Start in transformer");
            T configuration = getConfiguration(confStr);
            if (configuration == null) {
                log.error("Invalid transformer configuration");
                updateStatusToFailed(progress, "Failed to transform data.", null);
                return false;
            }
            return transform(progress, workflowDir, baseSources, baseVersions, baseTemplates, targetTemplate, configuration, confStr);
        } catch (Exception e) {
            log.error("Transformer failed to transform", e);
            updateStatusToFailed(progress, "Failed to transform data.", e);
            return false;
        }
    }

    protected void updateStatusToFailed(TransformationProgress progress, String errorMsg, Exception e) {
        progressHelper.updateStatusToFailed(progressEntityMgr, progress, errorMsg, e, getLogger());
    }

}
