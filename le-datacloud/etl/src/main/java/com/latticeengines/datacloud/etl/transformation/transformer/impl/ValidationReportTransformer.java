package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.datacloud.dataflow.SourceValidationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ValidationConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ValidationReportTransformerConfig;

@Component("validationReportTransformer")
public class ValidationReportTransformer extends AbstractDataflowTransformer<ValidationReportTransformerConfig, SourceValidationFlowParameters> {

    private static final Log log = LogFactory.getLog(AbstractTransformer.class);

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
        if (baseSources.size() != 1) {
            log.error("Validate only one result at a time");
            return false;
        }
        List<String> reportAttrs = config.getReportAttrs();
        if ((reportAttrs == null) || (reportAttrs.size() == 0)) {
            log.error("Must define some attrs to report");
            return false;
        }

        List<ValidationConfig> rules = config.getRules();
        if ((rules == null) || (rules.size() == 0)) {
            log.error("Must define at least one validation rule");
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
