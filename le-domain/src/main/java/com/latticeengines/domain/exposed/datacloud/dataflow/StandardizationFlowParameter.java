package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.ConsolidateIndustryStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.ConsolidateRangeStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.FieldType;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.IDStrategy;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.StandardizationTransformerConfig.StandardizationStrategy;

public class StandardizationFlowParameter extends TransformationFlowParameters {
    @JsonProperty("Sequence")
    private StandardizationStrategy[] sequence;

    @JsonProperty("DomainFields")
    private String[] domainFields;

    @JsonProperty("CountryFields")
    private String[] countryFields;

    @JsonProperty("StateFields")
    private String[] stateFields;

    @JsonProperty("StringFields")
    private String[] stringFields;

    @JsonProperty("ConvertTypeFields")
    private String[] convertTypeFields;

    @JsonProperty("ConvertTypeStrategies")
    private TypeConvertStrategy[] convertTypeStrategies;

    @JsonProperty("DedupFields")
    private String[] dedupFields;

    @JsonProperty("FilterExpression")
    private String filterExpression;

    @JsonProperty("FilterFields")
    private String[] filterFields;

    @JsonProperty("UploadTimestampField")
    private String uploadTimestampField;

    @JsonProperty("MarkerExpression")
    private String markerExpression;

    @JsonProperty("MarkerCheckFields")
    private String[] markerCheckFields;

    @JsonProperty("MarkerField")
    private String markerField;

    @JsonProperty("RenameFields")
    private String[][] renameFields;

    @JsonProperty("RetainFields")
    private String[] retainFields;

    @JsonProperty("DiscardFields")
    private String[] discardFields;

    @JsonProperty("AddFields")
    private String[] addFields;

    @JsonProperty("AddFieldValues")
    private Object[] addFieldValues;

    @JsonProperty("AddFieldTypes")
    private FieldType[] addFieldTypes;

    @JsonProperty("IsValidDomainField")
    private String isValidDomainField;

    @JsonProperty("ValidDomainCheckField")
    private String validDomainCheckField;

    @JsonProperty("AddConsolidatedIndustryField")
    private String addConsolidatedIndustryField;

    @JsonProperty("ConsolidateIndustryStrategy")
    private ConsolidateIndustryStrategy consolidateIndustryStrategy;

    @JsonProperty("IndustryField")
    private String industryField;

    @JsonProperty("NaicsField")
    private String naicsField;

    @JsonProperty("IndustryMapFileName")
    private String industryMapFileName;

    @JsonProperty("NaicsMapFileName")
    private String naicsMapFileName;

    @JsonProperty("AddConsolidatedRangeFields")
    private String[] addConsolidatedRangeFields;

    @JsonProperty("ConsolidateRangeStrategies")
    private ConsolidateRangeStrategy[] consolidateRangeStrategies;

    @JsonProperty("RangeInputFields")
    private String[] rangeInputFields;

    @JsonProperty("RangeMapFileNames")
    private String[] rangeMapFileNames;

    @JsonProperty("DunsFields")
    private String[] dunsFields;

    @JsonProperty("IDFields")
    private String[] idFields;

    @JsonProperty("IDStrategies")
    private IDStrategy[] idStrategies;

    @JsonProperty("CopyFields")
    private String[][] copyFields;

    @JsonProperty("ChecksumExcludeFields")
    private String[] checksumExcludeFields;

    @JsonProperty("ChecksumField")
    private String checksumField;

    @JsonProperty("UpdateFields")
    private String[] updateFields;

    @JsonProperty("UpdateExpressions")
    private String[] updateExpressions;

    @JsonProperty("UpdateInputFields")
    private String[][] updateInputFields; // for strategy UPDATE, fields used to
                                          // update target fields

    @JsonProperty("SampleFraction")
    private Float sampleFraction;

    @JsonProperty("StandardCountries")
    private Map<String, String> standardCountries;

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

    public String getUploadTimestampField() {
        return uploadTimestampField;
    }

    public void setUploadTimestampField(String uploadTimestampField) {
        this.uploadTimestampField = uploadTimestampField;
    }

    public Map<String, String> getStandardCountries() {
        return standardCountries;
    }

    public void setStandardCountries(Map<String, String> standardCountries) {
        this.standardCountries = standardCountries;
    }

    public String[] getFilterFields() {
        return filterFields;
    }

    public void setFilterFields(String[] filterFields) {
        this.filterFields = filterFields;
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

    public void setConsolidateIndustryStrategy(
            ConsolidateIndustryStrategy consolidateIndustryStrategy) {
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

    public void setConsolidateRangeStrategies(
            ConsolidateRangeStrategy[] consolidateRangeStrategies) {
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

    public String[][] getCopyFields() {
        return copyFields;
    }

    public void setCopyFields(String[][] copyFields) {
        this.copyFields = copyFields;
    }

    public String[] getChecksumExcludeFields() {
        return checksumExcludeFields;
    }

    public void setChecksumExcludeFields(String[] checksumExcludeFields) {
        this.checksumExcludeFields = checksumExcludeFields;
    }

    public String getChecksumField() {
        return checksumField;
    }

    public void setChecksumField(String checksumField) {
        this.checksumField = checksumField;
    }

    public String[] getUpdateFields() {
        return updateFields;
    }

    public void setUpdateFields(String[] updateFields) {
        this.updateFields = updateFields;
    }

    public String[] getUpdateExpressions() {
        return updateExpressions;
    }

    public void setUpdateExpressions(String[] updateExpressions) {
        this.updateExpressions = updateExpressions;
    }

    public String[][] getUpdateInputFields() {
        return updateInputFields;
    }

    public void setUpdateInputFields(String[][] updateInputFields) {
        this.updateInputFields = updateInputFields;
    }

    public Float getSampleFraction() {
        return sampleFraction;
    }

    public void setSampleFraction(Float sampleFraction) {
        this.sampleFraction = sampleFraction;
    }

}
