package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.TimeFilter.Period;

public class DateValueLookup extends ValueLookup {

    @JsonProperty("Period")
    private Period period;

    public DateValueLookup(Object value, Period period) {
        super(value);
        this.period = period;
    }

    public Period getPeriod() {
        return period;
    }

    public void setPeriod(Period period) {
        this.period = period;
    }
}
