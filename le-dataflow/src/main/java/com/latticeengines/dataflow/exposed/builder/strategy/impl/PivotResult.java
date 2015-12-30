package com.latticeengines.dataflow.exposed.builder.strategy.impl;

public class PivotResult {

    private final String columnName;
    private Object value;
    private final PivotType pivotType;

    public PivotResult(String columnName, PivotType pivotType) {
        this.columnName = columnName;
        this.pivotType = pivotType;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PivotResult) {
            PivotResult otherResult = (PivotResult) other;
            return this.getColumnName().equals(otherResult.getColumnName()) &&
                    this.getValue().equals(otherResult.getValue());
        } else {
            return false;
        }
    }

    public String getColumnName() { return columnName; }

    public Object getValue() { return value; }

    public void setValue(Object value) { this.value = value; }

    public PivotType getPivotType() { return pivotType; }
}
