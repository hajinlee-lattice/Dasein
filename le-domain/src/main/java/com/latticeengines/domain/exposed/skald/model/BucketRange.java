package com.latticeengines.domain.exposed.skald.model;

public class BucketRange {
    // The user facing display name of this bucket.
    public String name;

    // Lower inclusive probability bound for this bucket.
    public Double lower;

    // Upper exclusive probability bound for this bucket.
    public Double upper;

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((lower == null) ? 0 : lower.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((upper == null) ? 0 : upper.hashCode());
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
        BucketRange other = (BucketRange) obj;
        if (lower == null) {
            if (other.lower != null)
                return false;
        } else if (!lower.equals(other.lower))
            return false;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (upper == null) {
            if (other.upper != null)
                return false;
        } else if (!upper.equals(other.upper))
            return false;
        return true;
    }
}
