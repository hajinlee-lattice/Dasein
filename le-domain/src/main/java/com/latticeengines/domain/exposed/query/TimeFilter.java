package com.latticeengines.domain.exposed.query;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;

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
        return ever(PeriodStrategy.Template.Month.name());
    }

    public static TimeFilter ever(String period) {
        TimeFilter filter = new TimeFilter();
        filter.relation = ComparisonType.EVER;
        filter.period = period != null ? period : PeriodStrategy.Template.Month.name();
        filter.values = Collections.singletonList(-1);
        return filter;
    }

    public static TimeFilter priorOnly(int val, String period) {
        TimeFilter filter = new TimeFilter();
        filter.relation = ComparisonType.PRIOR_ONLY;
        filter.period = period != null ? period : PeriodStrategy.Template.Month.name();
        filter.values = Collections.singletonList(val);
        return filter;
    }

    public static TimeFilter within(int val, String period) {
        TimeFilter filter = new TimeFilter();
        filter.relation = ComparisonType.WITHIN;
        filter.period = period != null ? period : PeriodStrategy.Template.Month.name();
        filter.values = Collections.singletonList(val);
        return filter;
    }

    public static TimeFilter between(int begin, int end, String period) {
        TimeFilter filter = new TimeFilter();
        filter.relation = ComparisonType.BETWEEN;
        filter.period = period != null ? period : PeriodStrategy.Template.Month.name();
        filter.values = Arrays.asList(begin, end);
        return filter;
    }

    public TimeFilter() {
    }

    public TimeFilter(ComparisonType relation, List<Object> values) {
        this.relation = relation;
        this.values = values;
    }

    public TimeFilter(ComparisonType relation, String period, List<Object> values) {
        this(null, relation, period, values);
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

    @Deprecated
    public static class Period {

        public static final Period Date = new Period("Date");
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
