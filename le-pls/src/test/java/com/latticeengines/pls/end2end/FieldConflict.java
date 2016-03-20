package com.latticeengines.pls.end2end;

public class FieldConflict {

    private String fieldName;

    private Object modeledValue;

    private Object scoreApiValue;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public Object getExpectedValue() {
        return modeledValue;
    }

    public void setExpectedValue(Object modelTransformedValue) {
        this.modeledValue = modelTransformedValue;
    }

    public Object getScoreApiValue() {
        return scoreApiValue;
    }

    public void setScoreApiValue(Object scoreApiTransformedValue) {
        this.scoreApiValue = scoreApiTransformedValue;
    }
}
