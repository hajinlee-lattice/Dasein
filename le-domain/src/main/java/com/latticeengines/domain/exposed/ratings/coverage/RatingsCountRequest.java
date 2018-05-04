package com.latticeengines.domain.exposed.ratings.coverage;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RatingsCountRequest {
    private List<String> segmentIds;

    private List<String> ratingEngineIds;

    private List<RatingModelIdPair> ratingEngineModelIds;

    private List<SegmentIdAndModelRulesPair> segmentIdModelRules;

    private List<SegmentIdAndSingleRulePair> segmentIdAndSingleRules;

    private List<RatingIdLookupColumnPair> ratingIdLookupColumnPairs;

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

    public List<SegmentIdAndSingleRulePair> getSegmentIdAndSingleRules() {
        return segmentIdAndSingleRules;
    }

    public void setSegmentIdAndSingleRules(List<SegmentIdAndSingleRulePair> segmentIdAndSingleRules) {
        this.segmentIdAndSingleRules = segmentIdAndSingleRules;
    }

    public List<RatingIdLookupColumnPair> getRatingIdLookupColumnPairs() {
        return ratingIdLookupColumnPairs;
    }

    public void setRatingIdLookupColumnPairs(List<RatingIdLookupColumnPair> ratingIdLookupColumnPairs) {
        this.ratingIdLookupColumnPairs = ratingIdLookupColumnPairs;
    }

    public boolean isRestrictNotNullSalesforceId() {
        return restrictNotNullSalesforceId;
    }

    public void setRestrictNotNullSalesforceId(boolean restrictNotNullSalesforceId) {
        this.restrictNotNullSalesforceId = restrictNotNullSalesforceId;
    }
}
