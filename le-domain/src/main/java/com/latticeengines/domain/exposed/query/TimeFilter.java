package com.latticeengines.domain.exposed.query;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TimeFilter {

    @JsonIgnore
    private Lookup lhs;

    @JsonProperty("Cmp")
    private ComparisonType relation;

    @JsonProperty("Vals")
    private List<Object> values = Collections.emptyList();

    @JsonProperty("Period")
    private Period period;

    public static TimeFilter ever() {
        TimeFilter filter = new TimeFilter();
        filter.relation = ComparisonType.EVER;
        filter.period = Period.Day;
        filter.values = Collections.singletonList(-1);
        return filter;
    }

    public TimeFilter() {
    }

    public TimeFilter(ComparisonType relation, List<Object> values) {
        this.relation = relation;
        this.values = values;
    }

    public TimeFilter(ComparisonType relation, Period p, List<Object> values) {
        this(null, relation, p, values);
    }

    public TimeFilter(Lookup lhs, ComparisonType relation, Period period, List<Object> values) {
        this.lhs = lhs;
        this.relation = relation;
        this.period = period;
        this.values = values;
    }

    public ComparisonType getRelation() {
        return relation;
    }

    public List<Object> getValues() {
        return values;
    }

    public Lookup getLhs() {
        return lhs;
    }

    public void setLhs(Lookup lhs) {
        this.lhs = lhs;
    }

    public Period getPeriod() {
        return period;
    }

    public void setPeriod(Period period) {
        this.period = period;
    }

    public enum Period {
        Day, Week, Month, Quarter, Year, Custom;
    }
}
