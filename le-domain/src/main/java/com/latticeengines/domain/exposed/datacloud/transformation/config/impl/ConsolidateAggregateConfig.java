package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.PeriodStrategy;

public class ConsolidateAggregateConfig extends TransformerConfig {

    private List<String> sumFields;
    private List<String> sumLongFields;
    private List<String> countFields;
    private List<String> goupByFields;
    private String trxDateField;
    private String trxTimeField;
    private PeriodStrategy periodStrategy;

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

    public List<String> getGoupByFields() {
        return goupByFields;
    }

    public void setGoupByFields(List<String> goupByFields) {
        this.goupByFields = goupByFields;
    }

    public String getTrxDateField() {
        return trxDateField;
    }

    public void setTrxDateField(String trxDateField) {
        this.trxDateField = trxDateField;
    }

    public PeriodStrategy getPeriodStrategy() {
        return periodStrategy;
    }

    public void setPeriodStrategy(PeriodStrategy periodStrategy) {
        this.periodStrategy = periodStrategy;
    }

    public String getTrxTimeField() {
        return this.trxTimeField;
    }

    public void setTrxTimeField(String trxTimeField) {
        this.trxTimeField = trxTimeField;
    }
}
