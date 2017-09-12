package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.DomainValidation;
import com.latticeengines.datacloud.dataflow.transformation.SourceStandardizationFlow;
import com.latticeengines.domain.exposed.datacloud.dataflow.StandardizationFlowParameter;
import com.latticeengines.domain.exposed.datacloud.dataflow.TypeConvertStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component(SourceStandardizationFlow.TRANSFORMER_NAME)
public class StandardizationTransformer
        extends AbstractDataflowTransformer<StandardizationTransformerConfig, StandardizationFlowParameter> {

    private static final Logger log = LoggerFactory.getLogger(StandardizationTransformer.class);

    @Autowired
    private CountryCodeService countryCodeService;

    @Override
    protected String getDataFlowBeanName() {
        return SourceStandardizationFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getName() {
        return SourceStandardizationFlow.TRANSFORMER_NAME;
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
            case DUNS:
                if (config.getDunsFields() == null || config.getDunsFields().length == 0) {
                    log.error("Duns fields are required for duns standardization");
                    return false;
                }
                for (String dunsField : config.getDunsFields()) {
                    if (StringUtils.isEmpty(dunsField)) {
                        log.error("Empty string or null is not allowed for duns field");
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
            case CONVERT_TYPE:
                if (config.getConvertTypeFields() == null || config.getConvertTypeFields().length == 0
                        || config.getConvertTypeStrategies() == null || config.getConvertTypeStrategies().length == 0) {
                    log.error("ConvertTypeFields and ConvertTypeStrategies are required for type convertion");
                    return false;
                }
                if (config.getConvertTypeFields().length != config.getConvertTypeStrategies().length) {
                    log.error("Must provide same number of ConvertTypeFields and ConvertTypeStrategies");
                    return false;
                }
                for (String convertTypeField : config.getConvertTypeFields()) {
                    if (StringUtils.isEmpty(convertTypeField)) {
                        log.error("Empty string or null is not allowed for ConvertTypeField");
                        return false;
                    }
                }
                for (TypeConvertStrategy convertTypeStrategy : config.getConvertTypeStrategies()) {
                    if (convertTypeStrategy == null) {
                        log.error("Null is not allowed for ConvertTypeStrategy");
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
            case ADD_FIELD:
                if (config.getAddFields() == null || config.getAddFieldValues() == null
                        || config.getAddFieldTypes() == null || config.getAddFields().length == 0
                        || config.getAddFieldValues().length == 0 || config.getAddFieldTypes().length == 0
                        || config.getAddFields().length != config.getAddFieldValues().length
                        || config.getAddFields().length != config.getAddFieldTypes().length) {
                    log.error("AddFields, AddFieldValues and AddFieldTypes are all required for adding new columns");
                    return false;
                }
                for (int i = 0; i < config.getAddFields().length; i++) {
                    if (StringUtils.isEmpty(config.getAddFields()[i]) || config.getAddFieldTypes()[i] == null) {
                        log.error("Empty value or null is not allowed for AddField and AddFieldType");
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
            case DISCARD:
                if (config.getDiscardFields() == null || config.getDiscardFields().length == 0) {
                    log.error("DiscardFields are required to discard attributes from a source ");
                    return false;
                }
                for (String discardField : config.getDiscardFields()) {
                    if (StringUtils.isEmpty(discardField)) {
                        log.error("EMpty string or null is not allowed in DiscardFields");
                        return false;
                    }
                }
                break;
            case ADD_ID:
                if (config.getIdFields() == null || config.getIdFields().length == 0 || config.getIdStrategies() == null
                        || config.getIdStrategies().length == 0) {
                    log.error("ID fields and ID strategies cannot be empty");
                    return false;
                }
                if (config.getIdFields().length != config.getIdFields().length) {
                    log.error("ID fields and ID strategies are not equal in size");
                    return false;
                }
                for (int i = 0; i < config.getIdFields().length; i++) {
                    if (StringUtils.isBlank(config.getIdFields()[i])) {
                        log.error(String.format("%dth ID field is empty", i));
                        return false;
                    }
                    if (config.getIdStrategies()[i] == null) {
                        log.error(String.format("%dth ID strategy field is empty", i));
                        return false;
                    }
                }
                break;
            case COPY:
                if (config.getCopyFields() == null || config.getCopyFields().length == 0) {
                    log.error(
                            "Copy field mapping is required for copying field. Format: [[\"fromField1\", \"toField1\"],[\"fromField2\", \"toField2\"], ...]");
                    return false;
                }
                for (String[] copyFieldMap : config.getCopyFields()) {
                    if (copyFieldMap == null || copyFieldMap.length != 2 || StringUtils.isEmpty(copyFieldMap[0])
                            || StringUtils.isEmpty(copyFieldMap[1])) {
                        log.error(
                                "Empty string or null is not allowed for copy fields. Format: [[\"fromField1\", \"toField1\"],[\"fromField2\", \"toField2\"], ...]");
                        return false;
                    }
                }
                break;
            case CHECKSUM:
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
            Source targetTemplate, StandardizationTransformerConfig config, List<String> baseVersions) {
        parameters.setDomainFields(config.getDomainFields());
        parameters.setCountryFields(config.getCountryFields());
        parameters.setStateFields(config.getStateFields());
        parameters.setConvertTypeFields(config.getConvertTypeFields());
        parameters.setConvertTypeStrategies(config.getConvertTypeStrategies());
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
        parameters.setAddFields(config.getAddFields());
        parameters.setAddFieldValues(config.getAddFieldValues());
        parameters.setAddFieldTypes(config.getAddFieldTypes());
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
        parameters.setDiscardFields(config.getDiscardFields());
        parameters.setDunsFields(config.getDunsFields());
        parameters.setIdFields(config.getIdFields());
        parameters.setIdStrategies(config.getIdStrategies());
        parameters.setCopyFields(config.getCopyFields());
        parameters.setChecksumExcludeFields(config.getChecksumExcludeFields());
        parameters.setChecksumField(config.getChecksumField());
        parameters.setStandardCountries(countryCodeService.getStandardCountries());
    }

}
