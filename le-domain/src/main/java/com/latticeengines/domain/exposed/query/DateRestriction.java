package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DateRestriction extends Restriction {

    @JsonProperty("Attr")
    private AttributeLookup attr;

    @JsonProperty("Fltr")
    private TimeFilter timeFilter;

    public DateRestriction() {
    }

    public DateRestriction(AttributeLookup attr, TimeFilter timeFilter) {
        this.attr = attr;
        this.timeFilter = timeFilter;
    }

    public AttributeLookup getAttr() {
        return attr;
    }

    public void setAttr(AttributeLookup attr) {
        this.attr = attr;
    }

    public TimeFilter getTimeFilter() {
        return timeFilter;
    }

    public void setTimeFilter(TimeFilter timeFilter) {
        this.timeFilter = timeFilter;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
