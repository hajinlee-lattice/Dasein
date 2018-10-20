package com.latticeengines.domain.exposed.datafabric;

public class DynamoIndex {
    private String hashKeyAttr;
    private String rangeKeyAttr;
    private String hashKeyField;
    private String rangeKeyField;
    private String bucketKeyField;
    private String stampKeyField;

    public DynamoIndex() {

    }

    public DynamoIndex(String hashKeyAttr, String hashKeyField, String rangeKeyAttr,
            String rangeKeyField, String bucketKeyField, String stampKeyField) {
        this.hashKeyAttr = hashKeyAttr;
        this.hashKeyField = hashKeyField;
        this.rangeKeyAttr = rangeKeyAttr;
        this.rangeKeyField = rangeKeyField;
        this.bucketKeyField = bucketKeyField;
        this.stampKeyField = stampKeyField;
    }

    public String getHashKeyAttr() {
        return hashKeyAttr;
    }

    public void setHashKeyAttr(String hashKeyAttr) {
        this.hashKeyAttr = hashKeyAttr;
    }

    public String getHashKeyField() {
        return hashKeyField;
    }

    public void setHashKeyField(String hashKeyField) {
        this.hashKeyField = hashKeyField;
    }

    public String getRangeKeyAttr() {
        return rangeKeyAttr;
    }

    public void setRangeKeyAttr(String rangeKeyAttr) {
        this.rangeKeyAttr = rangeKeyAttr;
    }

    public String getRangeKeyField() {
        return rangeKeyField;
    }

    public void setRangeKeyField(String rangeKeyField) {
        this.rangeKeyField = rangeKeyField;
    }

    public String getBucketKeyField() {
        return bucketKeyField;
    }

    public void setBucketKeyField(String bucketKeyField) {
        this.bucketKeyField = bucketKeyField;
    }

    public String getStampKeyField() {
        return stampKeyField;
    }

    public void setStampKeyField(String stampKeyField) {
        this.stampKeyField = stampKeyField;
    }
}
