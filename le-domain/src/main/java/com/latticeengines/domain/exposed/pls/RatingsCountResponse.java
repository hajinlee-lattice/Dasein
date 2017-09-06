package com.latticeengines.domain.exposed.pls;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RatingsCountResponse {

    private Map<String, CoverageInfo> segmentIdCoverageMap;

    private Map<String, CoverageInfo> ratingEngineIdCoverageMap;

    private Map<RatingModelIdPair, CoverageInfo> ratingEngineModelIdCoverageMap;

    public Map<String, CoverageInfo> getSegmentIdCoverageMap() {
        return segmentIdCoverageMap;
    }

    public void setSegmentIdCoverageMap(Map<String, CoverageInfo> segmentIdCoverageMap) {
        this.segmentIdCoverageMap = segmentIdCoverageMap;
    }

    public Map<String, CoverageInfo> getRatingEngineIdCoverageMap() {
        return ratingEngineIdCoverageMap;
    }

    public void setRatingEngineIdCoverageMap(Map<String, CoverageInfo> ratingEngineIdCoverageMap) {
        this.ratingEngineIdCoverageMap = ratingEngineIdCoverageMap;
    }

    public Map<RatingModelIdPair, CoverageInfo> getRatingEngineModelIdCoverageMap() {
        return ratingEngineModelIdCoverageMap;
    }

    public void setRatingEngineModelIdCoverageMap(Map<RatingModelIdPair, CoverageInfo> ratingEngineModelIdCoverageMap) {
        this.ratingEngineModelIdCoverageMap = ratingEngineModelIdCoverageMap;
    }

}
