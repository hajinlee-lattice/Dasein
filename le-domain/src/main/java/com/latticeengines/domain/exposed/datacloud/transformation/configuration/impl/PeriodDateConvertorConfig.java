package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PeriodDateConvertorConfig extends TransformerConfig {

    @JsonProperty("TrxDayPeriodField")
    private String trxDayPeriodField;
    @JsonProperty("TrxDateField")
    private String trxDateField;
    @JsonProperty("TrxTimeField")
    private String trxTimeField;

    public void setTrxDayPeriodField(String trxDayPeriodField) {
        this.trxDayPeriodField = trxDayPeriodField;
    }

    public String getTrxDayPeriodField() {
        return trxDayPeriodField;
    }

    public void setTrxDateField(String trxDateField) {
        this.trxDateField = trxDateField;
    }

    public String getTrxDateField() {
        return trxDateField;
    }

    public String getTrxTimeField() {
        return this.trxTimeField;
    }

    public void setTrxTimeField(String trxTimeField) {
        this.trxTimeField = trxTimeField;
    }
}
