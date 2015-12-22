package com.latticeengines.dataflow.exposed.builder.pivot;

public class PivotResult {

    private final String columnName;
    private final int priority;
    private Object value;

    public PivotResult(String columnName, int priority) {
        this.columnName = columnName;
        this.priority = priority;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof PivotResult) {
            PivotResult otherResult = (PivotResult) other;
            return this.getColumnName().equals(otherResult.getColumnName())
                    && this.getPriority() == otherResult.getPriority();
        } else {
            return false;
        }
    }

    public int getPriority() { return priority; }
    public String getColumnName() { return columnName; }

    public Object getValue() { return value; }

    public void setValue(Object value) { this.value = value; }

}
