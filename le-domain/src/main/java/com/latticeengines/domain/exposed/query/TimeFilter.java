package com.latticeengines.domain.exposed.query;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

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
    private String period;

    public static TimeFilter ever() {
        TimeFilter filter = new TimeFilter();
        filter.relation = ComparisonType.EVER;
        filter.period = Period.Month.name();
        filter.values = Collections.singletonList(-1);
        return filter;
    }

    public TimeFilter() {
    }

    public TimeFilter(ComparisonType relation, List<Object> values) {
        this.relation = relation;
        this.values = values;
    }

    public TimeFilter(ComparisonType relation, String p, List<Object> values) {
        this(null, relation, p, values);
    }

    public TimeFilter(Lookup lhs, ComparisonType relation, String period, List<Object> values) {
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

    public String getPeriod() {
        return period;
    }

    public void setPeriod(String period) {
        this.period = period;
    }

    public static class Period {

        public static final Period Day = new Period("Day");
        public static final Period Week = new Period("Week");
        public static final Period Month = new Period("Month");
        public static final Period Quarter = new Period("Quarter");
        public static final Period Year = new Period("Year");

        private final String name;

        public Period(String name) {
            this.name = name;
        }

        public String name() {
            return name;
        }

        @Override
        public boolean equals(Object object) {
            return !(object == null || !(object instanceof Period)) && EqualsBuilder.reflectionEquals(this, object);
        }

        @Override
        public int hashCode() {
            return HashCodeBuilder.reflectionHashCode(this);
        }
    }
}
