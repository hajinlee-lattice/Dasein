package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig.FieldType;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig.StandardizationStrategy;

public class StandardizationFlowParameter extends TransformationFlowParameters {
    @JsonProperty("Sequence")
    private StandardizationStrategy[] sequence;

    @JsonProperty("DomainFields")
    private String[] domainFields;

    @JsonProperty("CountryFields")
    private String[] countryFields;

    @JsonProperty("StateFields")
    private String[] stateFields;

    @JsonProperty("StringToIntFields")
    private String[] stringToIntFields;

    @JsonProperty("StringToLongFields")
    private String[] stringToLongFields;

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

    @JsonProperty("AddNullFields")
    private String[] addNullFields;

    @JsonProperty("AddNullFieldTypes")
    private FieldType[] addNullFieldTypes;

    @JsonProperty("StandardCountries")
    private Map<String, String> standardCountries;

    public String[] getDomainFields() {
        return domainFields;
    }

    public void setDomainFields(String[] domainFields) {
        this.domainFields = domainFields;
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

    public String[] getStringToIntFields() {
        return stringToIntFields;
    }

    public void setStringToIntFields(String[] stringToIntFields) {
        this.stringToIntFields = stringToIntFields;
    }

    public String[] getStringToLongFields() {
        return stringToLongFields;
    }

    public void setStringToLongFields(String[] stringToLongFields) {
        this.stringToLongFields = stringToLongFields;
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

    public String[] getAddNullFields() {
        return addNullFields;
    }

    public void setAddNullFields(String[] addNullFields) {
        this.addNullFields = addNullFields;
    }

    public FieldType[] getAddNullFieldTypes() {
        return addNullFieldTypes;
    }

    public void setAddNullFieldTypes(FieldType[] addNullFieldTypes) {
        this.addNullFieldTypes = addNullFieldTypes;
    }

}
