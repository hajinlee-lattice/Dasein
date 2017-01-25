package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.datacloud.dataflow.StandardizationFlowParameter;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component("standardizationTransformer")
public class StandardizationTransformer
        extends AbstractDataflowTransformer<StandardizationTransformerConfig, StandardizationFlowParameter> {

    private static final Log log = LogFactory.getLog(StandardizationTransformer.class);

    private static String transfomerName = "standardizationTransformer";

    private static String dataFlowBeanName = "sourceStandardizationFlow";

    @Autowired
    private CountryCodeService countryCodeService;

    @Override
    protected String getDataFlowBeanName() {
        return dataFlowBeanName;
    }

    @Override
    public String getName() {
        return transfomerName;
    }

    @Override
    protected boolean validateConfig(StandardizationTransformerConfig config, List<String> baseSources) {
        if (baseSources.size() != 1) {
            log.error("Standardize only one source at a time");
            return false;
        }
        if (StringUtils.isNotEmpty(config.getFilterExpression())
                && (config.getFilterFields() == null || config.getFilterFields().length == 0)) {
            log.error("Filter fields are required for filtering");
            return false;
        }
        if ((config.getMarkerExpression() != null && config.getMarkerField() == null)
                || (config.getMarkerExpression() == null || config.getMarkerField() != null)) {
            log.error("Mark expression and mark field are both required for marker");
            return false;
        }
        return true;
    }

    @Override
    protected Class<? extends TransformerConfig> getConfigurationClass() {
        return StandardizationTransformerConfig.class;
    }

    @Override
    protected Class<StandardizationFlowParameter> getDataFlowParametersClass() {
        return StandardizationFlowParameter.class;
    }

    @Override
    protected void updateParameters(StandardizationFlowParameter parameters, Source[] baseTemplates,
            Source targetTemplate, StandardizationTransformerConfig config) {
        parameters.setDomainFields(config.getDomainFields());
        parameters.setAddOrReplaceDomainFields(config.getAddOrReplaceDomainFields());
        parameters.setCountryFields(config.getCountryFields());
        parameters.setAddOrReplaceCountryFields(config.getAddOrReplaceCountryFields());
        parameters.setStateFields(config.getStateFields());
        parameters.setAddOrReplaceStateFields(config.getAddOrReplaceStateFields());
        parameters.setStringToIntFields(config.getStringToIntFields());
        parameters.setAddOrReplaceStringToIntFields(config.getAddOrReplaceStringToIntFields());
        parameters.setStringToLongFields(config.getStringToLongFields());
        parameters.setAddOrReplaceStringToLongFields(config.getAddOrReplaceStringToLongFields());
        parameters.setDedupFields(config.getDedupFields());
        parameters.setFilterExpression(config.getFilterExpression());
        parameters.setFilterFields(config.getFilterFields());
        parameters.setUploadTimestampField(config.getUploadTimestampField());
        parameters.setMarkerExpression(config.getMarkerExpression());
        parameters.setMarkerCheckFields(config.getMarkerCheckFields());
        parameters.setMarkerField(config.getMarkerField());
        parameters.setStandardCountries(countryCodeService.getStandardCountries());
    }

}
