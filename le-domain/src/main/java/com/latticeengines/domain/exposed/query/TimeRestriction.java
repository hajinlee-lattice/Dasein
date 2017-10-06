package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TimeRestriction extends Restriction {

    @JsonProperty("filter")
    private TimeFilter filter;

    public TimeRestriction(TimeFilter filter) {
        this.filter = filter;
    }

    private TimeRestriction() {}

    public TimeFilter getFilter() {
        return filter;
    }

    public void setFilter(TimeFilter filter) {
        this.filter = filter;
    }

}
