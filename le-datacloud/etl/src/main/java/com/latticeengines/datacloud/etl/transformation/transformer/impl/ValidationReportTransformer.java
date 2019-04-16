package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.domain.exposed.datacloud.dataflow.SourceValidationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ValidationConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ValidationReportTransformerConfig;

@Component("validationReportTransformer")
public class ValidationReportTransformer extends AbstractDataflowTransformer<ValidationReportTransformerConfig, SourceValidationFlowParameters> {

    private static final Logger log = LoggerFactory.getLogger(AbstractTransformer.class);

    private static String transfomerName = "validationReportTransformer";

    private static String dataFlowBeanName = "sourceValidationFlow";

    @Override
    public String getName() {
        return transfomerName;
    }

    @Override
    protected String getDataFlowBeanName() {
        return dataFlowBeanName;
    }

    @Override
    public boolean validateConfig(ValidationReportTransformerConfig config, List<String> baseSources) {
        String error = null;
        if (baseSources.size() != 1) {
            error = "Validate only one result at a time";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        List<String> reportAttrs = config.getReportAttrs();
        if ((reportAttrs == null) || (reportAttrs.size() == 0)) {
            error = "Must define some attrs to report";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }

        List<ValidationConfig> rules = config.getRules();
        if ((rules == null) || (rules.size() == 0)) {
            error = "Must define at least one validation rule";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }

        return true;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return ValidationReportTransformerConfig.class;
    }

    @Override
    protected Class<SourceValidationFlowParameters> getDataFlowParametersClass() {
        return SourceValidationFlowParameters.class;
    }

    @Override
    protected void updateParameters(SourceValidationFlowParameters parameters, Source[] baseTemplates,
            Source targetTemplate, ValidationReportTransformerConfig config, List<String> baseVersions) {
        parameters.setReportAttrs(config.getReportAttrs());
        parameters.setRules(config.getRules());
    }
}
