package com.latticeengines.domain.exposed.pls;

public enum BucketName {

    A("A", 99, 95), //
    B("B", 94, 85), //
    C("C", 84, 50), //
    D("D", 49, 5), //
    A_PLUS("A+"), //
    E("E"), //
    F("F");//

    private String value;
    private int defaultUpperBound;
    private int defaultLowerBound;

    BucketName(String value) {
        this.value = value;
    }

    BucketName(String value, int defaultUpperBound, int defaultLowerBound) {
        this.value = value;
        this.defaultUpperBound = defaultUpperBound;
        this.defaultLowerBound = defaultLowerBound;
    }

    public static BucketName fromValue(String value) {
        for (BucketName bucketName : values()) {
            if (bucketName.toValue().equals(value)) {
                return bucketName;
            }
        }

        return null;
    }

    public String toValue() {
        return this.value;
    }

    public int getDefaultUpperBound() {
        return this.defaultUpperBound;
    }

    public int getDefaultLowerBound() {
        return this.defaultLowerBound;
    }

}
