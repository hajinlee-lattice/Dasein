package com.latticeengines.domain.exposed.pls;

public enum BucketName {

    A(99, 95), //
    B(94, 85), //
    C(84, 50), //
    D(49, 5), //
    E, //
    F;//

    private int defaultUpperBound;
    private int defaultLowerBound;

    private BucketName() {
    }

    private BucketName(int defaultUpperBound, int defaultLowerBound) {
        this.defaultUpperBound = defaultUpperBound;
        this.defaultLowerBound = defaultLowerBound;
    }

    public int getDefaultUpperBound() {
        return this.defaultUpperBound;
    }

    public int getDefaultLowerBound() {
        return this.defaultLowerBound;
    }

}
