package com.latticeengines.domain.exposed.datafabric;

public class DynamoIndex {
    private String hashKeyAttr;
    private String rangeKeyAttr;
    private String hashKeyField;
    private String rangeKeyField;

    public DynamoIndex() {

    }

    public DynamoIndex(String hashKeyAttr, String hashKeyField, String rangeKeyAttr, String rangeKeyField) {
        this.hashKeyAttr = hashKeyAttr;
        this.hashKeyField = hashKeyField;
        this.rangeKeyAttr = rangeKeyAttr;
        this.rangeKeyField = rangeKeyField;
    }

    public void setHashKeyAttr(String hashKeyAttr) {
        this.hashKeyAttr = hashKeyAttr;
    }

    public String getHashKeyAttr() {
        return hashKeyAttr;
    }

    public void setHashKeyField(String hashKeyField) {
        this.hashKeyField = hashKeyField;
    }

    public String getHashKeyField() {
        return hashKeyField;
    }

    public void setRangeKeyAttr(String rangeKeyAttr) {
        this.rangeKeyAttr = rangeKeyAttr;
    }

    public String getRangeKeyAttr() {
        return rangeKeyAttr;
    }

    public void setRangeKeyField(String rangeKeyField) {
        this.rangeKeyField = rangeKeyField;
    }

    public String getRangeKeyField() {
        return rangeKeyField;
    }
}

