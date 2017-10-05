package com.latticeengines.domain.exposed.pls;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RatingsCountResponse {

    private Map<String, CoverageInfo> segmentIdCoverageMap;

    private Map<String, CoverageInfo> ratingEngineIdCoverageMap;

    private Map<String, CoverageInfo> ratingEngineModelIdCoverageMap;

    private Map<String, CoverageInfo> segmentIdModelRulesCoverageMap;

    private Map<String, CoverageInfo> segmentIdAndSingleRulesCoverageMap;

    private Map<String, Map<String, String>> errorMap;

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

    public Map<String, CoverageInfo> getRatingEngineModelIdCoverageMap() {
        return ratingEngineModelIdCoverageMap;
    }

    public void setRatingEngineModelIdCoverageMap(Map<String, CoverageInfo> ratingEngineModelIdCoverageMap) {
        this.ratingEngineModelIdCoverageMap = ratingEngineModelIdCoverageMap;
    }

    public Map<String, CoverageInfo> getSegmentIdModelRulesCoverageMap() {
        return segmentIdModelRulesCoverageMap;
    }

    public void setSegmentIdModelRulesCoverageMap(Map<String, CoverageInfo> segmentIdModelRulesCoverageMap) {
        this.segmentIdModelRulesCoverageMap = segmentIdModelRulesCoverageMap;
    }

    public Map<String, CoverageInfo> getSegmentIdAndSingleRulesCoverageMap() {
        return segmentIdAndSingleRulesCoverageMap;
    }

    public void setSegmentIdAndSingleRulesCoverageMap(Map<String, CoverageInfo> segmentIdAndSingleRulesCoverageMap) {
        this.segmentIdAndSingleRulesCoverageMap = segmentIdAndSingleRulesCoverageMap;
    }

    public Map<String, Map<String, String>> getErrorMap() {
        return errorMap;
    }

    public void setErrorMap(Map<String, Map<String, String>> errorMap) {
        this.errorMap = errorMap;
    }
}
