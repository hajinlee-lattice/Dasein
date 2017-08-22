package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;

public class ConsolidateAggregateConfig extends TransformerConfig {

    private String sumField;
    private String countField;
    private List<String> goupByFields;
    private String trxDateField;

    public String getSumField() {

        return this.sumField;
    }

    public void setSumField(String sumField) {
        this.sumField = sumField;
    }

    public String getCountField() {
        return this.countField;
    }

    public void setCountField(String countField) {
        this.countField = countField;
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
}
