package com.latticeengines.domain.exposed.pls;

public enum RatingBucketName {
    A, B, C, D, E, F;

    public String getName() {
        return name();
    }

    public static String getUnscoredBucketName() {
        return "Unscored";
    }
}
