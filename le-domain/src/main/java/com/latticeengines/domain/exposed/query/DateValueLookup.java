package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DateValueLookup extends ValueLookup {

    @JsonProperty("Period")
    private String period;

    public DateValueLookup(Object value, String period) {
        super(value);
        this.period = period;
    }

    public String getPeriod() {
        return period;
    }

    public void setPeriod(String period) {
        this.period = period;
    }
}
