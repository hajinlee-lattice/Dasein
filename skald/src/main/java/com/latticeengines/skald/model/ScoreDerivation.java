package com.latticeengines.skald.model;

import java.util.List;

public class ScoreDerivation {
    // The average probability of the model.
    // This is necessary for calculating lift.
    public double averageProbability;

    public List<BucketRange> percentiles;

    public List<BucketRange> buckets;
}
