package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.dataflow.TypeConvertStrategy;

public class StandardizationTransformerConfig extends TransformerConfig {
    @JsonProperty("Sequence")
    private StandardizationStrategy[] sequence;     // Put operations in sequence array sequentially

    @JsonProperty("DomainFields")
    private String[] domainFields; // for strategy DOMAIN

    @JsonProperty("CountryFields")
    private String[] countryFields; // for strategy COUNTRY

    @JsonProperty("StateFields")
    private String[] stateFields; // for strategy STATE (Country standardization
                                  // must finish before state standardization;
                                  // Will use first country field as standard
                                  // country to standardize state)

    @JsonProperty("StringFields")
    private String[] stringFields; // for strategy STRING

    @JsonProperty("ConvertTypeFields")
    private String[] convertTypeFields; // for strategy CONVERT_TYPE

    @JsonProperty("ConvertTypeStrategies")
    private TypeConvertStrategy[] convertTypeStrategies;    // for strategy CONVERT_TYPE

    @JsonProperty("DedupFields")
    private String[] dedupFields; // for strategy DEDUP

    @JsonProperty("FilterExpression")
    private String filterExpression; // for strategy FILTER

    @JsonProperty("FilterFields")
    private String[] filterFields; // for strategy FILTER

    @JsonProperty("UploadTimestampField")
    private String uploadTimestampField; // for strategy UPLOAD_TIMESTAMP

    @JsonProperty("MarkerExpression")
    private String markerExpression; // for strategy MARKER

    @JsonProperty("MarkerCheckFields")
    private String[] markerCheckFields; // for strategy MARKER

    @JsonProperty("MarkerField")
    private String markerField; // for strategy MARKER

    @JsonProperty("RenameFields")
    private String[][] renameFields; // for strategy RENAME (String[][0] is old
                                     // name, String[][1] is new name)

    @JsonProperty("RetainFields")
    private String[] retainFields; // for strategy RETAIN

    @JsonProperty("DiscardFields")
    private String[] discardFields; // for strategy DISCARD

    @JsonProperty("AddFields")
    private String[] addFields; // for strategy ADD_FIELD

    @JsonProperty("AddFieldValues")
    private Object[] addFieldValues; // for strategy ADD_FIELD

    @JsonProperty("AddFieldTypes")
    private FieldType[] addFieldTypes; // for strategy ADD_FIELD

    @JsonProperty("IsValidDomainField")
    private String isValidDomainField; // for strategy VALID_DOMAIN

    @JsonProperty("ValidDomainCheckField")
    private String validDomainCheckField; // for strategy VALID_DOMAIN

    @JsonProperty("AddConsolidatedIndustryField")
    private String addConsolidatedIndustryField; // for strategy
                                                 // CONSOLIDATE_INDUSTRY

    @JsonProperty("ConsolidateIndustryStrategy")
    private ConsolidateIndustryStrategy consolidateIndustryStrategy;    // for strategy CONSOLIDATE_INDUSTRY

    @JsonProperty("IndustryField")
    private String industryField; // for strategy CONSOLIDATE_INDUSTRY

    @JsonProperty("NaicsField")
    private String naicsField; // for strategy CONSOLIDATE_INDUSTRY

    @JsonProperty("IndustryMapFileName")
    private String industryMapFileName; // for strategy CONSOLIDATE_INDUSTRY

    @JsonProperty("NaicsMapFileName")
    private String naicsMapFileName; // for strategy CONSOLIDATE_INDUSTRY

    @JsonProperty("AddConsolidatedRangeFields")
    private String[] addConsolidatedRangeFields; // for strategy
                                                 // CONSOLIDATE_RANGE

    @JsonProperty("ConsolidateRangeStrategies")
    private ConsolidateRangeStrategy[] consolidateRangeStrategies; // for strategy CONSOLIDATE_RANGE

    @JsonProperty("RangeInputFields")
    private String[] rangeInputFields; // for strategy CONSOLIDATE_RANGE

    @JsonProperty("RangeMapFileNames")
    private String[] rangeMapFileNames; // for strategy CONSOLIDATE_RANGE

    @JsonProperty("DunsFields")
    private String[] dunsFields; // for strategy DUNS

    @JsonProperty("IDFields")
    private String[] idFields; // for strategy ADD_ID

    @JsonProperty("IDStrategies")
    private IDStrategy[] idStrategies; // for strategy ADD_ID

    public enum StandardizationStrategy {
        ADD_ID, DOMAIN, DUNS, COUNTRY, STATE, STRING, CONVERT_TYPE, DEDUP, FILTER, UPLOAD_TIMESTAMP, MARKER, RENAME, RETAIN, DISCARD, ADD_FIELD, VALID_DOMAIN, CONSOLIDATE_INDUSTRY, CONSOLIDATE_RANGE
    }

    public enum FieldType {
        STRING, INT, LONG, BOOLEAN, FLOAT, DOUBLE
    }
    
    public enum ConsolidateIndustryStrategy {
        MAP_INDUSTRY, PARSE_NAICS
    }

    public enum ConsolidateRangeStrategy {
        MAP_RANGE, MAP_VALUE
    }

    public enum IDStrategy {
        ROWID, UUID
    }

    public String[] getDomainFields() {
        return domainFields;
    }

    public void setDomainFields(String[] domainFields) {
        this.domainFields = domainFields;
    }

    public String[] getDunsFields() {
        return dunsFields;
    }

    public void setDunsFields(String[] dunsFields) {
        this.dunsFields = dunsFields;
    }

    public String[] getCountryFields() {
        return countryFields;
    }

    public void setCountryFields(String[] countryFields) {
        this.countryFields = countryFields;
    }

    public String[] getStateFields() {
        return stateFields;
    }

    public void setStateFields(String[] stateFields) {
        this.stateFields = stateFields;
    }

    public String[] getStringFields() {
        return stringFields;
    }

    public void setStringFields(String[] stringFields) {
        this.stringFields = stringFields;
    }

    public String[] getConvertTypeFields() {
        return convertTypeFields;
    }

    public void setConvertTypeFields(String[] convertTypeFields) {
        this.convertTypeFields = convertTypeFields;
    }

    public TypeConvertStrategy[] getConvertTypeStrategies() {
        return convertTypeStrategies;
    }

    public void setConvertTypeStrategies(TypeConvertStrategy[] convertTypeStrategies) {
        this.convertTypeStrategies = convertTypeStrategies;
    }

    public String[] getDedupFields() {
        return dedupFields;
    }

    public void setDedupFields(String[] dedupFields) {
        this.dedupFields = dedupFields;
    }

    public String getFilterExpression() {
        return filterExpression;
    }

    public void setFilterExpression(String filterExpression) {
        this.filterExpression = filterExpression;
    }

    public String[] getFilterFields() {
        return filterFields;
    }

    public void setFilterFields(String[] filterFields) {
        this.filterFields = filterFields;
    }

    public String getUploadTimestampField() {
        return uploadTimestampField;
    }

    public void setUploadTimestampField(String uploadTimestampField) {
        this.uploadTimestampField = uploadTimestampField;
    }

    public String getMarkerExpression() {
        return markerExpression;
    }

    public void setMarkerExpression(String markerExpression) {
        this.markerExpression = markerExpression;
    }

    public String[] getMarkerCheckFields() {
        return markerCheckFields;
    }

    public void setMarkerCheckFields(String[] markerCheckFields) {
        this.markerCheckFields = markerCheckFields;
    }

    public String getMarkerField() {
        return markerField;
    }

    public void setMarkerField(String markerField) {
        this.markerField = markerField;
    }

    public String[][] getRenameFields() {
        return renameFields;
    }

    public void setRenameFields(String[][] renameFields) {
        this.renameFields = renameFields;
    }

    public String[] getRetainFields() {
        return retainFields;
    }

    public void setRetainFields(String[] retainFields) {
        this.retainFields = retainFields;
    }

    public StandardizationStrategy[] getSequence() {
        return sequence;
    }

    public void setSequence(StandardizationStrategy[] sequence) {
        this.sequence = sequence;
    }

    public String[] getAddFields() {
        return addFields;
    }

    public void setAddFields(String[] addFields) {
        this.addFields = addFields;
    }

    public Object[] getAddFieldValues() {
        return addFieldValues;
    }

    public void setAddFieldValues(Object[] addFieldValues) {
        this.addFieldValues = addFieldValues;
    }

    public FieldType[] getAddFieldTypes() {
        return addFieldTypes;
    }

    public void setAddFieldTypes(FieldType[] addFieldTypes) {
        this.addFieldTypes = addFieldTypes;
    }

    public String getIsValidDomainField() {
        return isValidDomainField;
    }

    public void setIsValidDomainField(String isValidDomainField) {
        this.isValidDomainField = isValidDomainField;
    }

    public String getValidDomainCheckField() {
        return validDomainCheckField;
    }

    public void setValidDomainCheckField(String validDomainCheckField) {
        this.validDomainCheckField = validDomainCheckField;
    }

    public String getAddConsolidatedIndustryField() {
        return addConsolidatedIndustryField;
    }

    public void setAddConsolidatedIndustryField(String addConsolidatedIndustryField) {
        this.addConsolidatedIndustryField = addConsolidatedIndustryField;
    }

    public ConsolidateIndustryStrategy getConsolidateIndustryStrategy() {
        return consolidateIndustryStrategy;
    }

    public void setConsolidateIndustryStrategy(ConsolidateIndustryStrategy consolidateIndustryStrategy) {
        this.consolidateIndustryStrategy = consolidateIndustryStrategy;
    }

    public String getIndustryField() {
        return industryField;
    }

    public void setIndustryField(String industryField) {
        this.industryField = industryField;
    }

    public String getNaicsField() {
        return naicsField;
    }

    public void setNaicsField(String naicsField) {
        this.naicsField = naicsField;
    }

    public String getIndustryMapFileName() {
        return industryMapFileName;
    }

    public void setIndustryMapFileName(String industryMapFileName) {
        this.industryMapFileName = industryMapFileName;
    }

    public String getNaicsMapFileName() {
        return naicsMapFileName;
    }

    public void setNaicsMapFileName(String naicsMapFileName) {
        this.naicsMapFileName = naicsMapFileName;
    }

    public String[] getAddConsolidatedRangeFields() {
        return addConsolidatedRangeFields;
    }

    public void setAddConsolidatedRangeFields(String[] addConsolidatedRangeFields) {
        this.addConsolidatedRangeFields = addConsolidatedRangeFields;
    }

    public ConsolidateRangeStrategy[] getConsolidateRangeStrategies() {
        return consolidateRangeStrategies;
    }

    public void setConsolidateRangeStrategies(ConsolidateRangeStrategy[] consolidateRangeStrategies) {
        this.consolidateRangeStrategies = consolidateRangeStrategies;
    }

    public String[] getRangeInputFields() {
        return rangeInputFields;
    }

    public void setRangeInputFields(String[] rangeInputFields) {
        this.rangeInputFields = rangeInputFields;
    }

    public String[] getRangeMapFileNames() {
        return rangeMapFileNames;
    }

    public void setRangeMapFileNames(String[] rangeMapFileNames) {
        this.rangeMapFileNames = rangeMapFileNames;
    }

    public String[] getDiscardFields() {
        return discardFields;
    }

    public void setDiscardFields(String[] discardFields) {
        this.discardFields = discardFields;
    }

    public String[] getIdFields() {
        return idFields;
    }

    public void setIdFields(String[] idFields) {
        this.idFields = idFields;
    }

    public IDStrategy[] getIdStrategies() {
        return idStrategies;
    }

    public void setIdStrategies(IDStrategy[] idStrategies) {
        this.idStrategies = idStrategies;
    }

}
