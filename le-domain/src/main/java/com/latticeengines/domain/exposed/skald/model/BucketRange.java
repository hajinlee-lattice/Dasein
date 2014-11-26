package com.latticeengines.domain.exposed.skald.model;

public class BucketRange {
    // The user facing display name of this bucket.
    public String name;

    // Lower inclusive probability bound for this bucket.
    public Double lower;

    // Upper exclusive probability bound for this bucket.
    public Double upper;
}
