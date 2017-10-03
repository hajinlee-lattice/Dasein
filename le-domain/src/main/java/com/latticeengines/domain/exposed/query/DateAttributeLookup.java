package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.TimeFilter.Period;

public class DateAttributeLookup extends AttributeLookup {

    @JsonProperty("Period")
    private Period period;

    public DateAttributeLookup(BusinessEntity entity, String attrName, Period period) {
        super(entity, attrName);
        this.period = period;
    }

    public DateAttributeLookup(AttributeLookup attrLookup, Period period) {
        super(attrLookup.getEntity(), attrLookup.getAttribute());
        this.period = period;
    }

    public Period getPeriod() {
        return period;
    }

    public void setPeriod(Period period) {
        this.period = period;
    }
}
