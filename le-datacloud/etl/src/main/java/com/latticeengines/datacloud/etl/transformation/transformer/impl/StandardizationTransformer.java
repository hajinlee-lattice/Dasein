package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.DomainValidation;
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
        if (baseSources == null || (baseSources.size() != 1
                && !(baseSources.size() == 2 && baseSources.get(1).equals(DomainValidation.class.getSimpleName())))) {
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
            case VALID_DOMAIN:
                if (baseSources.size() != 2 || !baseSources.get(1).equals(DomainValidation.class.getSimpleName())) {
                    log.error(
                            "The first base source should be the input data source and the second base source must be DomainValidation");
                    return false;
                }
                if (StringUtils.isEmpty(config.getIsValidDomainField())
                        || StringUtils.isEmpty(config.getValidDomainCheckField())) {
                    log.error("IsValidDomainField and ValidDomainCheckField are both required for domain validation");
                    return false;
                }
                break;
            case CONSOLIDATE_INDUSTRY:
                if (StringUtils.isEmpty(config.getAddConsolidatedIndustryField())) {
                    log.error("AddConsolidatedIndustryField is required for industry consolidation");
                    return false;
                }
                switch(config.getConsolidateIndustryStrategy()) {
                case MAP_INDUSTRY:
                    if (StringUtils.isEmpty(config.getIndustryField()) || StringUtils.isEmpty(config.getIndustryMapFileName())) {
                        log.error(String.format("For ConsolidateIndustryStrategy %s, IndustryField and IndustryMapFileName are required.", config.getConsolidateIndustryStrategy().name()));
                        return false;
                    }
                    break;
                case PARSE_NAICS:
                    if (StringUtils.isEmpty(config.getNaicsField()) || StringUtils.isEmpty(config.getNaicsMapFileName())) {
                        log.error(String.format("For ConsolidateIndustryStrategy %s, NaicsField and NaicsMapFileName are required.", config.getConsolidateIndustryStrategy().name()));
                        return false;
                    }
                    break;
                default:
                    log.error(String.format("ConsolidateIndustryStrategy %s is not supported", config.getConsolidateIndustryStrategy().name()));
                    return false;
                }
                break;
            case STATE:
                if (config.getCountryFields() == null || config.getCountryFields().length == 0
                        || StringUtils.isEmpty(config.getCountryFields()[0])) {
                    log.error("Must provide country field which has standard country information");
                    return false;
                }
                if (config.getStateFields() == null || config.getStateFields().length == 0) {
                    log.error("State fields are required for state standardization");
                    return false;
                }
                for (String stateField : config.getStateFields()) {
                    if (StringUtils.isEmpty(stateField)) {
                        log.error("Empty string or null is not allowed in state fields");
                        return false;
                    }
                }
                break;
            case STRING:
                if (config.getStringFields() == null || config.getStringFields().length == 0) {
                    log.error("String fields are required for string standardization");
                    return false;
                }
                for (String stringField : config.getStringFields()) {
                    if (StringUtils.isEmpty(stringField)) {
                        log.error("Empty string or null is not allowed in string fields");
                        return false;
                    }
                }
                break;
            case CONSOLIDATE_RANGE:
                if (config.getAddConsolidatedRangeFields() == null || config.getRangeInputFields() == null
                        || config.getConsolidateRangeStrategies() == null || config.getRangeMapFileNames() == null) {
                    log.error(
                            "AddConsolidatedRangeFields, RangeFields, ConsolidateRangeStrategies and RangeMapFileNames are required for range standardization");
                    return false;
                }
                if (!(config.getAddConsolidatedRangeFields().length > 0
                        && config.getAddConsolidatedRangeFields().length == config.getRangeInputFields().length
                        && config.getAddConsolidatedRangeFields().length == config
                                .getConsolidateRangeStrategies().length
                        && config.getAddConsolidatedRangeFields().length == config.getRangeMapFileNames().length)) {
                    log.error(
                            "Number of AddConsolidatedRangeFields, RangeFields, ConsolidateRangeStrategies and RangeMapFileNames should be the same and larger than 0");
                    return false;
                }
                for (String addConsolidatedRangeField : config.getAddConsolidatedRangeFields()) {
                    if (StringUtils.isEmpty(addConsolidatedRangeField)) {
                        log.error("Empty string or null is not allowed in AddConsolidatedRangeField");
                        return false;
                    }
                }
                for (String rangeField : config.getRangeInputFields()) {
                    if (StringUtils.isEmpty(rangeField)) {
                        log.error("Empty string or null is not allowed in RangeField");
                        return false;
                    }
                }
                for (StandardizationTransformerConfig.ConsolidateRangeStrategy rangeStrategy : config
                        .getConsolidateRangeStrategies()) {
                    if (rangeStrategy == null) {
                        log.error("Null is not allowed in ConsolidateRangeStrategy");
                        return false;
                    }
                }
                for (String fileName : config.getRangeMapFileNames()) {
                    if (StringUtils.isEmpty(fileName)) {
                        log.error("Empty string or null is not allowed in RangeMapFileName");
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
        parameters.setIsValidDomainField(config.getIsValidDomainField());
        parameters.setValidDomainCheckField(config.getValidDomainCheckField());
        parameters.setAddConsolidatedIndustryField(config.getAddConsolidatedIndustryField());
        parameters.setConsolidateIndustryStrategy(config.getConsolidateIndustryStrategy());
        parameters.setIndustryField(config.getIndustryField());
        parameters.setNaicsField(config.getNaicsField());
        parameters.setIndustryMapFileName(config.getIndustryMapFileName());
        parameters.setNaicsMapFileName(config.getNaicsMapFileName());
        parameters.setStateFields(config.getStateFields());
        parameters.setStringFields(config.getStringFields());
        parameters.setAddConsolidatedRangeFields(config.getAddConsolidatedRangeFields());
        parameters.setRangeInputFields(config.getRangeInputFields());
        parameters.setConsolidateRangeStrategies(config.getConsolidateRangeStrategies());
        parameters.setRangeMapFileNames(config.getRangeMapFileNames());
        parameters.setStandardCountries(countryCodeService.getStandardCountries());
    }

}
