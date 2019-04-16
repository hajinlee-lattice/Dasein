package com.latticeengines.datacloud.etl.transformation.transformer.impl;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.service.CountryCodeService;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.DomainValidation;
import com.latticeengines.datacloud.core.util.RequestContext;
import com.latticeengines.datacloud.dataflow.transformation.SourceStandardizationFlow;
import com.latticeengines.domain.exposed.datacloud.dataflow.StandardizationFlowParameter;
import com.latticeengines.domain.exposed.datacloud.dataflow.TypeConvertStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Table;

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
        String error = null;
        if (baseSources == null || (baseSources.size() != 1
                && !(baseSources.size() == 2 && baseSources.get(1).equals(DomainValidation.class.getSimpleName())))) {
            error = "Standardize only one source at a time";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        if (config.getSequence() == null || config.getSequence().length == 0) {
            error = "Standardization sequence is required";
            log.error(error);
            RequestContext.logError(error);
            return false;
        }
        for (StandardizationTransformerConfig.StandardizationStrategy strategy : config.getSequence()) {
            switch (strategy) {
            case DOMAIN:
                if (config.getDomainFields() == null || config.getDomainFields().length == 0) {
                    error = "Domain fields are required for domain standardization";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String domainField : config.getDomainFields()) {
                    if (StringUtils.isEmpty(domainField)) {
                        error = "Empty string or null is not allowed for domain field";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case DUNS:
                if (config.getDunsFields() == null || config.getDunsFields().length == 0) {
                    error = "Duns fields are required for duns standardization";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String dunsField : config.getDunsFields()) {
                    if (StringUtils.isEmpty(dunsField)) {
                        error = "Empty string or null is not allowed for duns field";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case COUNTRY:
                if (config.getCountryFields() == null || config.getCountryFields().length == 0) {
                    error = "Country fields are required for country standardization";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String countryField : config.getCountryFields()) {
                    if (StringUtils.isEmpty(countryField)) {
                        error = "Empty string or null is not allowed for country field";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case CONVERT_TYPE:
                if (config.getConvertTypeFields() == null || config.getConvertTypeFields().length == 0
                        || config.getConvertTypeStrategies() == null || config.getConvertTypeStrategies().length == 0) {
                    error = "ConvertTypeFields and ConvertTypeStrategies are required for type convertion";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                if (config.getConvertTypeFields().length != config.getConvertTypeStrategies().length) {
                    error = "Must provide same number of ConvertTypeFields and ConvertTypeStrategies";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String convertTypeField : config.getConvertTypeFields()) {
                    if (StringUtils.isEmpty(convertTypeField)) {
                        error = "Empty string or null is not allowed for ConvertTypeField";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                for (TypeConvertStrategy convertTypeStrategy : config.getConvertTypeStrategies()) {
                    if (convertTypeStrategy == null) {
                        error = "Null is not allowed for ConvertTypeStrategy";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case DEDUP:
                if (config.getDedupFields() == null || config.getDedupFields().length == 0) {
                    error = "Dedup fields are required for dedup";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String dedupField : config.getDedupFields()) {
                    if (StringUtils.isEmpty(dedupField)) {
                        error = "Empty string or null is not allowed for dedup field";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case FILTER:
                if (StringUtils.isEmpty(config.getFilterExpression()) || config.getFilterFields() == null
                        || config.getFilterFields().length == 0) {
                    error = "Filter expression and filter fields are both required for filtering";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String filterField : config.getFilterFields()) {
                    if (StringUtils.isEmpty(filterField)) {
                        error = "Empty string or null is not allowed for filter field";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case UPLOAD_TIMESTAMP:
                if (StringUtils.isEmpty(config.getUploadTimestampField())) {
                    error = "UploadTimestamp field is required for addding UploadTimestamp";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                break;
            case MARKER:
                if (StringUtils.isEmpty(config.getMarkerExpression()) || StringUtils.isEmpty(config.getMarkerField())
                        || config.getMarkerCheckFields() == null || config.getMarkerCheckFields().length == 0) {
                    error = "Marker expression, marker field and marker checking fields are all required for adding marker";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String markerCheckField : config.getMarkerCheckFields()) {
                    if (StringUtils.isEmpty(markerCheckField)) {
                        error = "Empty string or null is not allowed for marker checking field";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case RENAME:
                if (config.getRenameFields() == null || config.getRenameFields().length == 0) {
                    error = "Renamed field mapping is required for renaming field. Format: [[\"OldName1\", \"NewName1\"],[\"OldName2\", \"NewName2\"], ...]";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String[] renameFieldMap : config.getRenameFields()) {
                    if (renameFieldMap == null || renameFieldMap.length != 2 || StringUtils.isEmpty(renameFieldMap[0])
                            || StringUtils.isEmpty(renameFieldMap[1])) {
                        error = "Empty string or null is not allowed for renamed fields. Format: [[\"OldName1\", \"NewName1\"],[\"OldName2\", \"NewName2\"], ...]";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case RETAIN:
                if (config.getRetainFields() == null || config.getRetainFields().length == 0) {
                    error = "Retained fields are required for retaining fields";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String retainField : config.getRetainFields()) {
                    if (StringUtils.isEmpty(retainField)) {
                        error = "Empty string or null is not allowed for retaind fields";
                        log.error(error);
                        RequestContext.logError(error);
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
                    error = "AddFields, AddFieldValues and AddFieldTypes are all required for adding new columns";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (int i = 0; i < config.getAddFields().length; i++) {
                    if (StringUtils.isEmpty(config.getAddFields()[i]) || config.getAddFieldTypes()[i] == null) {
                        error = "Empty value or null is not allowed for AddField and AddFieldType";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case VALID_DOMAIN:
                if (baseSources.size() != 2 || !baseSources.get(1).equals(DomainValidation.class.getSimpleName())) {
                    error = "The first base source should be the input data source and the second base source must be DomainValidation";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                if (StringUtils.isEmpty(config.getIsValidDomainField())
                        || StringUtils.isEmpty(config.getValidDomainCheckField())) {
                    error = "IsValidDomainField and ValidDomainCheckField are both required for domain validation";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                break;
            case CONSOLIDATE_INDUSTRY:
                if (StringUtils.isEmpty(config.getAddConsolidatedIndustryField())) {
                    error = "AddConsolidatedIndustryField is required for industry consolidation";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                switch(config.getConsolidateIndustryStrategy()) {
                case MAP_INDUSTRY:
                    if (StringUtils.isEmpty(config.getIndustryField()) || StringUtils.isEmpty(config.getIndustryMapFileName())) {
                        error = String.format(
                                "For ConsolidateIndustryStrategy %s, IndustryField and IndustryMapFileName are required.",
                                config.getConsolidateIndustryStrategy().name());
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                    break;
                case PARSE_NAICS:
                    if (StringUtils.isEmpty(config.getNaicsField()) || StringUtils.isEmpty(config.getNaicsMapFileName())) {
                        error = String.format(
                                "For ConsolidateIndustryStrategy %s, NaicsField and NaicsMapFileName are required.",
                                config.getConsolidateIndustryStrategy().name());
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                    break;
                default:
                    error = String.format("ConsolidateIndustryStrategy %s is not supported",
                            config.getConsolidateIndustryStrategy().name());
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                break;
            case STATE:
                if (config.getCountryFields() == null || config.getCountryFields().length == 0
                        || StringUtils.isEmpty(config.getCountryFields()[0])) {
                    error = "Must provide country field which has standard country information";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                if (config.getStateFields() == null || config.getStateFields().length == 0) {
                    error = "State fields are required for state standardization";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String stateField : config.getStateFields()) {
                    if (StringUtils.isEmpty(stateField)) {
                        error = "Empty string or null is not allowed in state fields";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case STRING:
                if (config.getStringFields() == null || config.getStringFields().length == 0) {
                    error = "String fields are required for string standardization";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String stringField : config.getStringFields()) {
                    if (StringUtils.isEmpty(stringField)) {
                        error = "Empty string or null is not allowed in string fields";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case CONSOLIDATE_RANGE:
                if (config.getAddConsolidatedRangeFields() == null || config.getRangeInputFields() == null
                        || config.getConsolidateRangeStrategies() == null || config.getRangeMapFileNames() == null) {
                    error = "AddConsolidatedRangeFields, RangeFields, ConsolidateRangeStrategies and RangeMapFileNames are required for range standardization";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                if (!(config.getAddConsolidatedRangeFields().length > 0
                        && config.getAddConsolidatedRangeFields().length == config.getRangeInputFields().length
                        && config.getAddConsolidatedRangeFields().length == config
                                .getConsolidateRangeStrategies().length
                        && config.getAddConsolidatedRangeFields().length == config.getRangeMapFileNames().length)) {
                    error = "Number of AddConsolidatedRangeFields, RangeFields, ConsolidateRangeStrategies and RangeMapFileNames should be the same and larger than 0";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String addConsolidatedRangeField : config.getAddConsolidatedRangeFields()) {
                    if (StringUtils.isEmpty(addConsolidatedRangeField)) {
                        error = "Empty string or null is not allowed in AddConsolidatedRangeField";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                for (String rangeField : config.getRangeInputFields()) {
                    if (StringUtils.isEmpty(rangeField)) {
                        error = "Empty string or null is not allowed in RangeField";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                for (StandardizationTransformerConfig.ConsolidateRangeStrategy rangeStrategy : config
                        .getConsolidateRangeStrategies()) {
                    if (rangeStrategy == null) {
                        error = "Null is not allowed in ConsolidateRangeStrategy";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                for (String fileName : config.getRangeMapFileNames()) {
                    if (StringUtils.isEmpty(fileName)) {
                        error = "Empty string or null is not allowed in RangeMapFileName";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case DISCARD:
                if (config.getDiscardFields() == null || config.getDiscardFields().length == 0) {
                    error = "DiscardFields are required to discard attributes from a source";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String discardField : config.getDiscardFields()) {
                    if (StringUtils.isEmpty(discardField)) {
                        error = "Empty string or null is not allowed in DiscardFields";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case ADD_ID:
                if (config.getIdFields() == null || config.getIdFields().length == 0 || config.getIdStrategies() == null
                        || config.getIdStrategies().length == 0) {
                    error = "ID fields and ID strategies cannot be empty";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                if (config.getIdFields().length != config.getIdFields().length) {
                    error = "ID fields and ID strategies are not equal in size";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (int i = 0; i < config.getIdFields().length; i++) {
                    if (StringUtils.isBlank(config.getIdFields()[i])) {
                        error = String.format("%dth ID field is empty", i);
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                    if (config.getIdStrategies()[i] == null) {
                        error = String.format("%dth ID strategy field is empty", i);
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case COPY:
                if (config.getCopyFields() == null || config.getCopyFields().length == 0) {
                    error = "Copy field mapping is required for copying field. Format: [[\"fromField1\", \"toField1\"],[\"fromField2\", \"toField2\"], ...]";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (String[] copyFieldMap : config.getCopyFields()) {
                    if (copyFieldMap == null || copyFieldMap.length != 2 || StringUtils.isEmpty(copyFieldMap[0])
                            || StringUtils.isEmpty(copyFieldMap[1])) {
                        error = "Empty string or null is not allowed for copy fields. Format: [[\"fromField1\", \"toField1\"],[\"fromField2\", \"toField2\"], ...]";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                }
                break;
            case CHECKSUM:
                break;
            case UPDATE:
                if (config.getUpdateFields() == null || config.getUpdateFields().length == 0
                        || config.getUpdateExpressions() == null || config.getUpdateExpressions().length == 0
                        || config.getUpdateInputFields() == null || config.getUpdateInputFields().length == 0) {
                    error = "UpdateFields, UpdateInputFields and UpdateExpressions cannot be empty";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                if (config.getUpdateFields().length != config.getUpdateExpressions().length
                        || config.getUpdateExpressions().length != config.getUpdateInputFields().length) {
                    error = "UpdateFields, UpdateInputFields and UpdateExpressions are not in same size";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                for (int i = 0; i < config.getUpdateFields().length; i++) {
                    if (StringUtils.isBlank(config.getUpdateFields()[i])
                            || StringUtils.isBlank(config.getUpdateExpressions()[i])
                            || config.getUpdateInputFields()[i] == null
                            || config.getUpdateInputFields()[i].length == 0) {
                        error = "UpdateFields, UpdateInputFields and UpdateExpressions cannot be empty or empty strings";
                        log.error(error);
                        RequestContext.logError(error);
                        return false;
                    }
                    for (String updateInputField : config.getUpdateInputFields()[i]) {
                        if (StringUtils.isBlank(updateInputField)) {
                            error = "UpdateInputFields cannot be empty strings";
                            log.error(error);
                            RequestContext.logError(error);
                            return false;
                        }
                    }
                }
                break;
            case SAMPLE:
                if (config.getSampleFraction() == null) {
                    error = "SampleFraction cannot be empty";
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                if ((config.getSampleFraction() <= 0) || (config.getSampleFraction() >= 1)) {
                    error = "SampleFraction must be within range (0, 1) -- bound exclusive, but got "
                            + config.getSampleFraction();
                    log.error(error);
                    RequestContext.logError(error);
                    return false;
                }
                break;
            default:
                error = String.format("Standardization strategy %s is not supported", strategy.name());
                log.error(error);
                RequestContext.logError(error);
                return false;
            }
        }
        return true;
    }

    @Override
    protected Schema getTargetSchema(Table result, StandardizationFlowParameter parameters,
            StandardizationTransformerConfig config, List<Schema> baseSchemas) {
        // respect isSyncSchemaProp for backward compatibility
        config.setShouldInheritSchemaProp(config.isSyncSchemaProp());
        return super.getTargetSchema(result, parameters, config, baseSchemas);
    }

    @Override
    protected boolean needBaseAvsc() {
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
        parameters.setUpdateFields(config.getUpdateFields());
        parameters.setUpdateExpressions(config.getUpdateExpressions());
        parameters.setUpdateInputFields(config.getUpdateInputFields());
        parameters.setSampleFraction(config.getSampleFraction());
        parameters.setStandardCountries(countryCodeService.getStandardCountries());
    }

}
