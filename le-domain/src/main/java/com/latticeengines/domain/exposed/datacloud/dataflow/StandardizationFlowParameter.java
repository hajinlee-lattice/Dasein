package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.StandardizationTransformerConfig.StandardizationStrategy;

public class StandardizationFlowParameter extends TransformationFlowParameters {
    @JsonProperty("DomainFields")
    private String[] domainFields;

    @JsonProperty("AddOrReplaceDomainFields")
    private StandardizationStrategy addOrReplaceDomainFields;

    @JsonProperty("CountryFields")
    private String[] countryFields;

    @JsonProperty("AddOrReplaceCountryFields")
    private StandardizationStrategy addOrReplaceCountryFields;

    @JsonProperty("StateFields")
    private String[] stateFields;

    @JsonProperty("AddOrReplaceStateFields")
    private StandardizationStrategy addOrReplaceStateFields;

    @JsonProperty("StringToIntFields")
    private String[] stringToIntFields;

    @JsonProperty("AddOrReplaceStringToIntFields")
    private StandardizationStrategy addOrReplaceStringToIntFields;

    @JsonProperty("StringToLongFields")
    private String[] stringToLongFields;

    @JsonProperty("AddOrReplaceStringToLongFields")
    private StandardizationStrategy addOrReplaceStringToLongFields;

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

    @JsonProperty("StandardCountries")
    private Map<String, String> standardCountries;

    public String[] getDomainFields() {
        return domainFields;
    }

    public void setDomainFields(String[] domainFields) {
        this.domainFields = domainFields;
    }

    public StandardizationStrategy getAddOrReplaceDomainFields() {
        return addOrReplaceDomainFields;
    }

    public void setAddOrReplaceDomainFields(StandardizationStrategy addOrReplaceDomainFields) {
        this.addOrReplaceDomainFields = addOrReplaceDomainFields;
    }

    public String[] getCountryFields() {
        return countryFields;
    }

    public void setCountryFields(String[] countryFields) {
        this.countryFields = countryFields;
    }

    public StandardizationStrategy getAddOrReplaceCountryFields() {
        return addOrReplaceCountryFields;
    }

    public void setAddOrReplaceCountryFields(StandardizationStrategy addOrReplaceCountryFields) {
        this.addOrReplaceCountryFields = addOrReplaceCountryFields;
    }

    public String[] getStateFields() {
        return stateFields;
    }

    public void setStateFields(String[] stateFields) {
        this.stateFields = stateFields;
    }

    public StandardizationStrategy getAddOrReplaceStateFields() {
        return addOrReplaceStateFields;
    }

    public void setAddOrReplaceStateFields(StandardizationStrategy addOrReplaceStateFields) {
        this.addOrReplaceStateFields = addOrReplaceStateFields;
    }

    public String[] getStringToIntFields() {
        return stringToIntFields;
    }

    public void setStringToIntFields(String[] stringToIntFields) {
        this.stringToIntFields = stringToIntFields;
    }

    public StandardizationStrategy getAddOrReplaceStringToIntFields() {
        return addOrReplaceStringToIntFields;
    }

    public void setAddOrReplaceStringToIntFields(StandardizationStrategy addOrReplaceStringToIntFields) {
        this.addOrReplaceStringToIntFields = addOrReplaceStringToIntFields;
    }

    public String[] getStringToLongFields() {
        return stringToLongFields;
    }

    public void setStringToLongFields(String[] stringToLongFields) {
        this.stringToLongFields = stringToLongFields;
    }

    public StandardizationStrategy getAddOrReplaceStringToLongFields() {
        return addOrReplaceStringToLongFields;
    }

    public void setAddOrReplaceStringToLongFields(StandardizationStrategy addOrReplaceStringToLongFields) {
        this.addOrReplaceStringToLongFields = addOrReplaceStringToLongFields;
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

}
