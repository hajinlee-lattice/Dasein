package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.domain.exposed.datacloud.dataflow.SourceSampleFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SampleTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component("sampleDataTransformer")
public class SampleDataTransformer extends AbstractDataflowTransformer<SampleTransformerConfig, SourceSampleFlowParameters> {

    private static final Logger log = LoggerFactory.getLogger(AbstractTransformer.class);

    private static String transfomerName = "sampleDataTransformer";

    private static String dataFlowBeanName = "sourceSampleFlow";

    @Override
    public String getName() {
        return transfomerName;
    }

    @Override
    protected String getDataFlowBeanName() {
        return dataFlowBeanName;
    }

    @Override
    public boolean validateConfig(SampleTransformerConfig config, List<String> baseSources) {
        String error = null;
        if (baseSources.size() != 1) {
            error = "Sample only one result at a time";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        Float fraction = config.getFraction();
        if ((fraction <= 0) || (fraction >= 1)) {
            error = "Invalid sample fraction " + fraction;
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        return true;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return SampleTransformerConfig.class;
    }

    @Override
    protected Class<SourceSampleFlowParameters> getDataFlowParametersClass() {
        return SourceSampleFlowParameters.class;
    }

    @Override
    protected void updateParameters(SourceSampleFlowParameters parameters, Source[] baseTemplates,
            Source targetTemplate, SampleTransformerConfig config, List<String> baseSources) {
        parameters.setFraction(config.getFraction());
        parameters.setFilter(config.getFilter());
        parameters.setFilterAttrs(config.getReportAttrs());
    }
}
