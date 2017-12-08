package com.latticeengines.domain.exposed.datacloud.check;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class AbstractGroupCheckParam extends CheckParam {

    @JsonProperty("GroupByFields")
    private List<String> groupByFields;

    @JsonProperty("Status")
    private String status;

    @JsonProperty("CountByField")
    private String countByField;

    @JsonProperty("CheckNullField")
    private String checkNullField;

    @JsonProperty("PrevVersionNullField")
    private String prevVersionNullField;

    @JsonProperty("CurrVersionNullField")
    private String currVersionNullField;

    @JsonProperty("KeyField")
    private String keyField;

    @JsonProperty("PopulationThreshold")
    private double threshold;

    @JsonProperty("ExceedCountThreshold")
    private int exceedCountThreshold;

    @JsonProperty("CoverageFields")
    private List<String> coverageFields;

    @JsonProperty("PrevVersionNotNullField")
    private String prevVersionNotNullField;

    @JsonProperty("CurrVersionNotNullField")
    private String currVersionNotNullField;

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getCountByField() {
        return countByField;
    }

    public void setCountByField(String countByField) {
        this.countByField = countByField;
    }

    public String getPrevVersionNullField() {
        return prevVersionNullField;
    }

    public void setPrevVersionNullField(String prevVersionNullField) {
        this.prevVersionNullField = prevVersionNullField;
    }

    public String getCurrVersionNullField() {
        return currVersionNullField;
    }

    public void setCurrVersionNullField(String currVersionNullField) {
        this.currVersionNullField = currVersionNullField;
    }

    public String getKeyField() {
        return keyField;
    }

    public void setKeyField(String keyField) {
        this.keyField = keyField;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public List<String> getCoverageFields() {
        return coverageFields;
    }

    public void setCoverageFields(List<String> coverageFields) {
        this.coverageFields = coverageFields;
    }

    public int getExceedCountThreshold() {
        return exceedCountThreshold;
    }

    public void setExceedCountThreshold(int exceedCountThreshold) {
        this.exceedCountThreshold = exceedCountThreshold;
    }

    public String getPrevVersionNotNullField() {
        return prevVersionNotNullField;
    }

    public void setPrevVersionNotNullField(String prevVersionNotNullField) {
        this.prevVersionNotNullField = prevVersionNotNullField;
    }

    public String getCurrVersionNotNullField() {
        return currVersionNotNullField;
    }

    public void setCurrVersionNotNullField(String currVersionNotNullField) {
        this.currVersionNotNullField = currVersionNotNullField;
    }

    public String getCheckNullField() {
        return checkNullField;
    }

    public void setCheckNullField(String checkNullField) {
        this.checkNullField = checkNullField;
    }

}
