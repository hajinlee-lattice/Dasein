package com.latticeengines.propdata.dataflow.depivot;

public class DepivotFieldMapping {
    private String[] targetFields;
    private String[][] sourceFieldTuples;

    public DepivotFieldMapping() {
        super();
    }

    public DepivotFieldMapping(String[] targetFields, String[][] sourceFieldTuples) {
        this();
        this.targetFields = targetFields;
        this.sourceFieldTuples = sourceFieldTuples;
    }

    public String[] getTargetFields() {
        return targetFields;
    }

    public void setTargetFields(String[] targetFields) {
        this.targetFields = targetFields;
    }

    public String[][] getSourceFieldTuples() {
        return sourceFieldTuples;
    }

    public void setSourceFieldTuples(String[][] sourceFieldTuples) {
        this.sourceFieldTuples = sourceFieldTuples;
    }
}