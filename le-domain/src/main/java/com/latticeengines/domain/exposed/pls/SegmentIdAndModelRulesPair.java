package com.latticeengines.domain.exposed.pls;

import com.latticeengines.common.exposed.util.JsonUtils;

public class SegmentIdAndModelRulesPair {
    private String segmentId;

    private RatingRule ratingRule;

    public String getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(String segmentId) {
        this.segmentId = segmentId;
    }

    public RatingRule getRatingRule() {
        return ratingRule;
    }

    public void setRatingRule(RatingRule ratingRule) {
        this.ratingRule = ratingRule;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((ratingRule == null) ? 0 : ratingRule.toString().hashCode());
        result = prime * result + ((segmentId == null) ? 0 : segmentId.hashCode());
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
        SegmentIdAndModelRulesPair other = (SegmentIdAndModelRulesPair) obj;
        if (ratingRule == null) {
            if (other.ratingRule != null)
                return false;
        } else if (!ratingRule.toString().equals(other.ratingRule.toString()))
            return false;
        if (segmentId == null) {
            if (other.segmentId != null)
                return false;
        } else if (!segmentId.equals(other.segmentId))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
