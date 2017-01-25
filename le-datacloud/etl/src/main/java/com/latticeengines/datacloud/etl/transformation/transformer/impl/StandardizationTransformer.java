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
        if (config.getSequence() == null || config.getSequence().length == 0) {
            log.error("Standardization sequence is required");
            return false;
        }
        for (StandardizationTransformerConfig.StandardizationStrategy strategy : config.getSequence()) {
            switch (strategy) {
            case DOMAIN:
                if (config.getDomainFields() == null || config.getDomainFields().length == 0) {
                    log.error("Domain fields are required for domain standardization");
                    return false;
                }
                for (String domainField : config.getDomainFields()) {
                    if (StringUtils.isEmpty(domainField)) {
                        log.error("Empty string or null is not allowed for domain field");
                        return false;
                    }
                }
                break;
            case COUNTRY:
                if (config.getCountryFields() == null || config.getCountryFields().length == 0) {
                    log.error("Country fields are required for country standardization");
                    return false;
                }
                for (String countryField : config.getCountryFields()) {
                    if (StringUtils.isEmpty(countryField)) {
                        log.error("Empty string or null is not allowed for country field");
                        return false;
                    }
                }
                break;
            case STRING_TO_INT:
                if (config.getStringToIntFields() == null || config.getStringToIntFields().length == 0) {
                    log.error("StringToInt fields are required for string to int type convertion");
                    return false;
                }
                for (String stringToIntField : config.getStringToIntFields()) {
                    if (StringUtils.isEmpty(stringToIntField)) {
                        log.error("Empty string or null is not allowed for StringToInt field");
                        return false;
                    }
                }
                break;
            case STRING_TO_LONG:
                if (config.getStringToLongFields() == null || config.getStringToLongFields().length == 0) {
                    log.error("StringToLong fields are required for string to long type convertion");
                    return false;
                }
                for (String stringToLongField : config.getStringToLongFields()) {
                    if (StringUtils.isEmpty(stringToLongField)) {
                        log.error("Empty string or null is not allowed for StringToLong field");
                        return false;
                    }
                }
                break;
            case DEDUP:
                if (config.getDedupFields() == null || config.getDedupFields().length == 0) {
                    log.error("Dedup fields are required for dedup");
                    return false;
                }
                for (String dedupField : config.getDedupFields()) {
                    if (StringUtils.isEmpty(dedupField)) {
                        log.error("Empty string or null is not allowed for dedup field");
                        return false;
                    }
                }
                break;
            case FILTER:
                if (StringUtils.isEmpty(config.getFilterExpression()) || config.getFilterFields() == null
                        || config.getFilterFields().length == 0) {
                    log.error("Filter expression and filter fields are both required for filtering");
                    return false;
                }
                for (String filterField : config.getFilterFields()) {
                    if (StringUtils.isEmpty(filterField)) {
                        log.error("Empty string or null is not allowed for filter field");
                        return false;
                    }
                }
                break;
            case UPLOAD_TIMESTAMP:
                if (StringUtils.isEmpty(config.getUploadTimestampField())) {
                    log.error("UploadTimestamp field is required for addding UploadTimestamp");
                    return false;
                }
                break;
            case MARKER:
                if (StringUtils.isEmpty(config.getMarkerExpression()) || StringUtils.isEmpty(config.getMarkerField())
                        || config.getMarkerCheckFields() == null || config.getMarkerCheckFields().length == 0) {
                    log.error(
                            "Marker expression, marker field and marker checking fields are all required for adding marker");
                    return false;
                }
                for (String markerCheckField : config.getMarkerCheckFields()) {
                    if (StringUtils.isEmpty(markerCheckField)) {
                        log.error("Empty string or null is not allowed for marker checking field");
                        return false;
                    }
                }
                break;
            case RENAME:
                if (config.getRenameFields() == null || config.getRenameFields().length == 0) {
                    log.error(
                            "Renamed field mapping is required for renaming field. Format: [[\"OldName1\", \"NewName1\"],[\"OldName2\", \"NewName2\"], ...]");
                    return false;
                }
                for (String[] renameFieldMap : config.getRenameFields()) {
                    if (renameFieldMap == null || renameFieldMap.length != 2 || StringUtils.isEmpty(renameFieldMap[0])
                            || StringUtils.isEmpty(renameFieldMap[1])) {
                        log.error(
                                "Empty string or null is not allowed for renamed fields. Format: [[\"OldName1\", \"NewName1\"],[\"OldName2\", \"NewName2\"], ...]");
                        return false;
                    }
                }
                break;
            case RETAIN:
                if (config.getRetainFields() == null || config.getRetainFields().length == 0) {
                    log.error("Retained fields are required for retaining fields");
                    return false;
                }
                for (String retainField : config.getRetainFields()) {
                    if (StringUtils.isEmpty(retainField)) {
                        log.error("Empty string or null is not allowed for retaind fields");
                        return false;
                    }
                }
                break;
            case ADD_NULL_FIELD:
                if (config.getAddNullFields() == null || config.getAddNullFieldTypes() == null
                        || config.getAddNullFields().length == 0 || config.getAddNullFieldTypes().length == 0
                        || config.getAddNullFields().length != config.getAddNullFieldTypes().length) {
                    log.error("AddNullFields and AddNullFieldTypes are both required for adding new null columns");
                    return false;
                }
                for (int i = 0; i < config.getAddNullFields().length; i++) {
                    if (StringUtils.isEmpty(config.getAddNullFields()[i]) || config.getAddNullFieldTypes()[i] == null) {
                        log.error("Empty value or null is not allowed for AddNullField and AddNullFieldType");
                        return false;
                    }
                }
                break;
            default:
                log.error(String.format("Standardization strategy %s is not supported", strategy.name()));
                return false;
            }
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
        parameters.setCountryFields(config.getCountryFields());
        parameters.setStateFields(config.getStateFields());
        parameters.setStringToIntFields(config.getStringToIntFields());
        parameters.setStringToLongFields(config.getStringToLongFields());
        parameters.setDedupFields(config.getDedupFields());
        parameters.setFilterExpression(config.getFilterExpression());
        parameters.setFilterFields(config.getFilterFields());
        parameters.setUploadTimestampField(config.getUploadTimestampField());
        parameters.setMarkerExpression(config.getMarkerExpression());
        parameters.setMarkerCheckFields(config.getMarkerCheckFields());
        parameters.setMarkerField(config.getMarkerField());
        parameters.setRenameFields(config.getRenameFields());
        parameters.setRetainFields(config.getRetainFields());
        parameters.setSequence(config.getSequence());
        parameters.setAddNullFields(config.getAddNullFields());
        parameters.setAddNullFieldTypes(config.getAddNullFieldTypes());
        parameters.setStandardCountries(countryCodeService.getStandardCountries());
    }

}
