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
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
