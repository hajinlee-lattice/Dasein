package com.latticeengines.domain.exposed.ratings.coverage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.RatingRule;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
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
