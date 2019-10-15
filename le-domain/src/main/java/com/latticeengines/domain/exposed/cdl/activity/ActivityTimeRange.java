package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.latticeengines.domain.exposed.query.ComparisonType;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ActivityTimeRange implements Serializable {
    private static final long serialVersionUID = 0L;

    // e.g. WITHIN (2 weeks), BETWEEN (param_1 and param_2)
    private ComparisonType operator;

    // e.g. [Week, Day]
    private Set<String> periods;

    // e.g. [[2], [4], [8], [12]]
    private Set<List<Integer>> paramSet;

    public ComparisonType getOperator() {
        return operator;
    }

    public void setOperator(ComparisonType operator) {
        this.operator = operator;
    }

    public Set<String> getPeriods() {
        return periods;
    }

    public void setPeriods(Set<String> periods) {
        this.periods = periods;
    }

    public Set<List<Integer>> getParamSet() {
        return paramSet;
    }

    public void setParamSet(Set<List<Integer>> paramSet) {
        this.paramSet = paramSet;
    }
}
