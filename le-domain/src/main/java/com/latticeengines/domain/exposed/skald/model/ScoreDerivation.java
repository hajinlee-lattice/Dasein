package com.latticeengines.domain.exposed.skald.model;

import java.util.List;

public class ScoreDerivation {
    // The PMML predicted field to look use as the predicted probability.
    // Required only if the PMML model has multiple predicted fields.
    public String target;

    // The average probability of the model.
    // This is necessary for calculating lift.
    public double averageProbability;

    public List<BucketRange> percentiles;

    public List<BucketRange> buckets;
}
