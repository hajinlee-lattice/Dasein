package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.period.PeriodStrategy;

public class PeriodConvertorConfig extends TransformerConfig {

    @JsonProperty("TrxDateField")
    private String trxDateField;

    @JsonProperty("PeriodField")
    private String periodField;

    @JsonProperty("PeriodStrategy")
    private PeriodStrategy periodStrategy;

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

    public void setPeriodStrategy(PeriodStrategy periodStrategy) {
        this.periodStrategy = periodStrategy;
    }

    public PeriodStrategy getPeriodStrategy() {
        return periodStrategy;
    }
}
