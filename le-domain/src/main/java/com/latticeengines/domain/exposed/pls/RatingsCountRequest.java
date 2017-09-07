package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RatingsCountRequest {
    private List<String> segmentIds;

    private List<String> ratingEngineIds;

    private List<RatingModelIdPair> ratingEngineModelIds;

    private List<SegmentIdAndModelRulesPair> segmentIdModelRules;

    private boolean restrictNotNullSalesforceId;

    public List<String> getSegmentIds() {
        return segmentIds;
    }

    public void setSegmentIds(List<String> segmentIds) {
        this.segmentIds = segmentIds;
    }

    public List<String> getRatingEngineIds() {
        return ratingEngineIds;
    }

    public void setRatingEngineIds(List<String> ratingEngineIds) {
        this.ratingEngineIds = ratingEngineIds;
    }

    public List<RatingModelIdPair> getRatingEngineModelIds() {
        return ratingEngineModelIds;
    }

    public void setRatingEngineModelIds(List<RatingModelIdPair> ratingEngineModelIds) {
        this.ratingEngineModelIds = ratingEngineModelIds;
    }

    public List<SegmentIdAndModelRulesPair> getSegmentIdModelRules() {
        return segmentIdModelRules;
    }

    public void setSegmentIdModelRules(List<SegmentIdAndModelRulesPair> segmentIdModelRules) {
        this.segmentIdModelRules = segmentIdModelRules;
    }

    public boolean isRestrictNotNullSalesforceId() {
        return restrictNotNullSalesforceId;
    }

    public void setRestrictNotNullSalesforceId(boolean restrictNotNullSalesforceId) {
        this.restrictNotNullSalesforceId = restrictNotNullSalesforceId;
    }
}
