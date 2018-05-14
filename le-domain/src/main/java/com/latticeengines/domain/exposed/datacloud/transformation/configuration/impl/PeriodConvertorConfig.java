package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;

public class PeriodConvertorConfig extends TransformerConfig {

    @JsonProperty("TrxDateField")
    private String trxDateField;

    @JsonProperty("PeriodField")
    private String periodField;

    @JsonProperty("PeriodStrategies")
    private List<PeriodStrategy> periodStrategies;

    public void setTrxDateField(String trxDateField) {
        this.trxDateField = trxDateField;
    }

    public String getPeriodField() {
        return periodField;
    }

    public void setPeriodField(String periodField) {
        this.periodField = periodField;
    }

    public String getTrxDateField() {
        return trxDateField;
    }

    public List<PeriodStrategy> getPeriodStrategies() {
        return periodStrategies;
    }

    public void setPeriodStrategies(List<PeriodStrategy> periodStrategies) {
        this.periodStrategies = periodStrategies;
    }

}
