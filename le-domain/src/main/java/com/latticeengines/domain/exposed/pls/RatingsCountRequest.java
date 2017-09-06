package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RatingsCountRequest {
    private List<String> segmentIds;

    private List<String> ratingEngineIds;

    private List<RatingModelIdPair> ratingEngineModelIds;

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

}
