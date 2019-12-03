package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.etl.transformation.service.impl.IngestedFileToSourceDataFlowService;
import com.latticeengines.datacloud.etl.transformation.transformer.TransformStep;
import com.latticeengines.domain.exposed.datacloud.dataflow.IngestedFileToSourceParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.IngestedFileToSourceTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(IngestedFileToSourceTransformer.TRANSFORMER_NAME)
public class IngestedFileToSourceTransformer
        extends AbstractDataflowTransformer<IngestedFileToSourceTransformerConfig, IngestedFileToSourceParameters> {

    private static final Logger log = LoggerFactory.getLogger(IngestedFileToSourceTransformer.class);

    public static final String TRANSFORMER_NAME = "ingestedFileToSourceTransformer";

    @Autowired
    private IngestedFileToSourceDataFlowService dataFlowService;

    @Override
    protected String getDataFlowBeanName() {
        return null;
    }

    @Override
    public String getName() {
        return TRANSFORMER_NAME;
    }

    @Override
    protected boolean validateConfig(IngestedFileToSourceTransformerConfig config, List<String> baseSources) {
        String error = null;
        if (baseSources.size() != 1) {
            error = "Process one ingestion at a time";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        if (StringUtils.isEmpty(config.getFileNameOrExtension())) {
            error = "Please provide file name or extension";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        return true;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return IngestedFileToSourceTransformerConfig.class;
    }

    @Override
    protected Class<IngestedFileToSourceParameters> getDataFlowParametersClass() {
        return IngestedFileToSourceParameters.class;
    }

    @Override
    protected void updateParameters(IngestedFileToSourceParameters parameters, Source[] baseTemplates,
            Source targetTemplate, IngestedFileToSourceTransformerConfig config, List<String> baseVersions) {
        parameters.setIngestionName(((IngestionSource) baseTemplates[0]).getIngestionName());
        parameters.setQualifier(config.getQualifier());
        parameters.setDelimiter(config.getDelimiter());
        parameters.setCharset(config.getCharset());
        parameters.setFileNameOrExtension(config.getFileNameOrExtension());
        parameters.setCompressedFileNameOrExtension(config.getCompressedFileNameOrExtension());
        parameters.setCompressType(config.getCompressType());
    }

    @Override
    protected boolean transformInternal(TransformationProgress progress, String workflowDir, TransformStep step) {
        try {
            Source[] baseSources = step.getBaseSources();
            List<String> baseSourceVersions = step.getBaseVersions();
            Source[] baseTemplates = step.getBaseTemplates();
            Source targetTemplate = step.getTargetTemplate();
            String confStr = step.getConfig();
            IngestedFileToSourceTransformerConfig configuration = getConfiguration(confStr);
            IngestedFileToSourceParameters parameters = getParameters(progress, baseSources, baseTemplates,
                    targetTemplate, configuration, confStr, baseSourceVersions);
            parameters.setApplicationId(progress.getYarnAppId());
            dataFlowService.executeDataFlow(targetTemplate, workflowDir, baseSourceVersions.get(0), parameters);
        } catch (Exception e) {
            log.error("Failed to transform data", e);
            return false;
        }

        return true;
    }
}
