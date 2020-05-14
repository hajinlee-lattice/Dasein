package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ActivityRowReducer implements Serializable {

    private static final long serialVersionUID = 0L;

    @JsonProperty
    private List<String> groupByFields; // columns used for composite pk of rows for reducing

    @JsonProperty
    private Operator operator;

    @JsonProperty
    private List<String> arguments; // specify columns controlling reducing behavior

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public void setArguments(List<String> arguments) {
        this.arguments = arguments;
    }

    /**
     * time-related operations: Earliest, Latest - reduce rows by groupByFields, pick rows with earliest/latest argument. preserve all columns if these operator defined
     */
    public enum Operator {
        Earliest,
        Latest,
        RandomOne // pick first one in group
    }
}
