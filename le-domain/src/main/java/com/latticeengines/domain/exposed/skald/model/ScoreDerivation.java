package com.latticeengines.domain.exposed.skald.model;

import java.util.List;

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
        final int prime = 31;
        int result = 1;
        long temp;
        temp = Double.doubleToLongBits(averageProbability);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        result = prime * result + ((buckets == null) ? 0 : buckets.hashCode());
        result = prime * result + ((percentiles == null) ? 0 : percentiles.hashCode());
        result = prime * result + ((target == null) ? 0 : target.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ScoreDerivation other = (ScoreDerivation) obj;
        if (Double.doubleToLongBits(averageProbability) != Double.doubleToLongBits(other.averageProbability))
            return false;
        if (buckets == null) {
            if (other.buckets != null)
                return false;
        } else if (!buckets.equals(other.buckets))
            return false;
        if (percentiles == null) {
            if (other.percentiles != null)
                return false;
        } else if (!percentiles.equals(other.percentiles))
            return false;
        if (target == null) {
            if (other.target != null)
                return false;
        } else if (!target.equals(other.target))
            return false;
        return true;
    }
}
