package com.latticeengines.domain.exposed.scoringapi;

import java.util.List;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

// Contains all the information necessary to construct score elements that
// are derived from the probability.
public class ScoreDerivation {
    public ScoreDerivation(String target, double averageProbability, List<BucketRange> percentiles,
            List<BucketRange> buckets) {
        this.target = target;
        this.averageProbability = averageProbability;
        this.percentiles = percentiles;
        this.buckets = buckets;
    }

    // Serialization constructor.
    public ScoreDerivation() {
    }

    // The PMML predicted field to look use as the predicted probability.
    // Required only if the PMML model has multiple predicted fields.
    public String target;

    // The average probability of the model.
    // This is necessary for calculating lift.
    public double averageProbability;

    public List<BucketRange> percentiles;

    public List<BucketRange> buckets;

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this);
    }
}
