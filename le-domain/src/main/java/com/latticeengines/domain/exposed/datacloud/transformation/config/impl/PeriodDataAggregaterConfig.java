package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PeriodDataAggregaterConfig extends TransformerConfig {

    @JsonProperty("SumFields")
    private List<String> sumFields;

    @JsonProperty("SumLongFields")
    private List<String> sumLongFields;

    @JsonProperty("CountFields")
    private List<String> countFields;

    @JsonProperty("SumOutputFields")
    private List<String> sumOutputFields;

    @JsonProperty("SumLongOutputFields")
    private List<String> sumLongOutputFields;

    @JsonProperty("CountOutputFields")
    private List<String> countOutputFields;

    @JsonProperty("GroupByFields")
    private List<String> groupByFields;

    public List<String> getSumFields() {
        return this.sumFields;
    }

    public void setSumFields(List<String> sumFields) {
        this.sumFields = sumFields;
    }

    public List<String> getSumLongFields() {
        return sumLongFields;
    }

    public void setSumLongFields(List<String> sumLongFields) {
        this.sumLongFields = sumLongFields;
    }

    public List<String> getCountFields() {
        return this.countFields;
    }

    public void setCountField(List<String> countFields) {
        this.countFields = countFields;
    }

    public List<String> getSumOutputFields() {
        return this.sumOutputFields;
    }

    public void setSumOutputFields(List<String> sumOutputFields) {
        this.sumOutputFields = sumOutputFields;
    }

    public List<String> getSumLongOutputFields() {
        return sumLongOutputFields;
    }

    public void setSumLongOutputFields(List<String> sumLongOutputFields) {
        this.sumLongOutputFields = sumLongOutputFields;
    }

    public List<String> getCountOutputFields() {
        return this.countOutputFields;
    }

    public void setCountOutputField(List<String> countOutputFields) {
        this.countOutputFields = countOutputFields;
    }

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
    }

}
