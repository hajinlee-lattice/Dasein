package com.latticeengines.domain.exposed.datacloud.check;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class AbstractGroupCheckParam extends CheckParam {

    @JsonProperty("GroupByFields")
    private List<String> groupByFields;

    @JsonProperty("Status")
    private Object status;

    @JsonProperty("CountByField")
    private Object countByField;

    @JsonProperty("CheckEmptyField")
    private Object checkEmptyField;

    @JsonProperty("PrevVersionEmptyField")
    private Object prevVersionEmptyField;

    @JsonProperty("CurrVersionEmptyField")
    private Object currVersionEmptyField;

    @JsonProperty("KeyField")
    private String keyField;

    @JsonProperty("PopulationThreshold")
    private double threshold;

    @JsonProperty("ExceedCountThreshold")
    private long exceedCountThreshold;

    @JsonProperty("ExpectedFieldValues")
    private List<Object> expectedFieldValues;

    @JsonProperty("PrevVersionNotEmptyField")
    private Object prevVersionNotEmptyField;

    @JsonProperty("CurrVersionNotEmptyField")
    private Object currVersionNotEmptyField;

    @JsonProperty("CntLessThanThresholdFlag") // indicates if cnt is less than threshold
    private boolean cntLessThanFlagThreshold;

    @JsonProperty("IdentifierFields")
    List<String> identifierFields;

    @JsonProperty("checkDupWithStatus") // check if check is on particular status
    private Boolean checkDupWithStatus;

    public Boolean getCheckDupWithStatus() {
        return checkDupWithStatus;
    }

    public void setCheckDupWithStatus(Boolean checkDupWithStatus) {
        this.checkDupWithStatus = checkDupWithStatus;
    }

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
    }

    public Object getStatus() {
        return status;
    }

    public void setStatus(Object status) {
        this.status = status;
    }

    public Object getCountByField() {
        return countByField;
    }

    public void setCountByField(Object countByField) {
        this.countByField = countByField;
    }

    public Object getPrevVersionEmptyField() {
        return prevVersionEmptyField;
    }

    public void setPrevVersionEmptyField(Object prevVersionEmptyField) {
        this.prevVersionEmptyField = prevVersionEmptyField;
    }

    public Object getCurrVersionEmptyField() {
        return currVersionEmptyField;
    }

    public void setCurrVersionNullField(Object currVersionEmptyField) {
        this.currVersionEmptyField = currVersionEmptyField;
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

    public List<Object> getExpectedFieldValues() {
        return expectedFieldValues;
    }

    public void setExpectedFieldValues(List<Object> expectedFieldValues) {
        this.expectedFieldValues = expectedFieldValues;
    }

    public long getExceedCountThreshold() {
        return exceedCountThreshold;
    }

    public void setExceedCountThreshold(long exceedCountThreshold) {
        this.exceedCountThreshold = exceedCountThreshold;
    }

    public Object getPrevVersionNotEmptyField() {
        return prevVersionNotEmptyField;
    }

    public void setPrevVersionNotEmptyField(Object prevVersionNotEmptyField) {
        this.prevVersionNotEmptyField = prevVersionNotEmptyField;
    }

    public Object getCurrVersionNotEmptyField() {
        return currVersionNotEmptyField;
    }

    public void setCurrVersionNotEmptyField(Object currVersionNotEmptyField) {
        this.currVersionNotEmptyField = currVersionNotEmptyField;
    }

    public Object getCheckEmptyField() {
        return checkEmptyField;
    }

    public void setCheckEmptyField(Object checkEmptyField) {
        this.checkEmptyField = checkEmptyField;
    }

    public boolean getCntLessThanThresholdFlag() {
        return cntLessThanFlagThreshold;
    }

    public void setCntLessThanThresholdFlag(boolean cntLessThanThresholdFlag) {
        this.cntLessThanFlagThreshold = cntLessThanThresholdFlag;
    }

    public List<String> getIdentifierFields() {
        return identifierFields;
    }

    public void setIdentifierFields(List<String> identifierFields) {
        this.identifierFields = identifierFields;
    }

}
