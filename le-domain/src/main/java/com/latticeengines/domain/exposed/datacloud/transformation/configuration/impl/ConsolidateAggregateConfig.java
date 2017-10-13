package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;

import com.latticeengines.common.exposed.period.PeriodStrategy;

public class ConsolidateAggregateConfig extends TransformerConfig {

    private List<String> sumFields;
    private List<String> countFields;
    private List<String> goupByFields;
    private String trxDateField;
    private PeriodStrategy periodStrategy;

    public List<String> getSumFields() {

        return this.sumFields;
    }

    public void setSumFields(List<String> sumFields) {
        this.sumFields = sumFields;
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

    public void setTrxDateField(String trxDateField) {
        this.trxDateField = trxDateField;
    }

    public String getTrxDateField() {
        return trxDateField;
    }

    public void setPeriodStrategy(PeriodStrategy periodStrategy) {
        this.periodStrategy = periodStrategy;
    }

    public PeriodStrategy getPeriodStrategy() {
        return periodStrategy;
    }
}
