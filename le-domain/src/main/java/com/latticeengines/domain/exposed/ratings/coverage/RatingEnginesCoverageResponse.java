package com.latticeengines.domain.exposed.ratings.coverage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RatingEnginesCoverageResponse {

    private Map<String, CoverageInfo> ratingModelsCoverageMap;

    private Map<String, String> errorMap;

    public RatingEnginesCoverageResponse() {
        ratingModelsCoverageMap = new ConcurrentHashMap<>();
        errorMap = new ConcurrentHashMap<>();
    }

    public Map<String, CoverageInfo> getRatingModelsCoverageMap() {
        return ratingModelsCoverageMap;
    }

    public void setRatingModelsCoverageMap(Map<String, CoverageInfo> ratingModelsCoverageMap) {
        this.ratingModelsCoverageMap = ratingModelsCoverageMap;
    }

    public Map<String, String> getErrorMap() {
        return errorMap;
    }

    public void setErrorMap(Map<String, String> errorMap) {
        this.errorMap = errorMap;
    }
}
