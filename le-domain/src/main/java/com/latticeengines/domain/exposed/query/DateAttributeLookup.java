package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DateAttributeLookup extends AttributeLookup {

    @JsonProperty("Period")
    private String period;

    public DateAttributeLookup(AttributeLookup attrLookup, String period) {
        super(attrLookup.getEntity(), attrLookup.getAttribute());
        this.period = period;
    }

    public String getPeriod() {
        return period;
    }

    public void setPeriod(String period) {
        this.period = period;
    }
}
