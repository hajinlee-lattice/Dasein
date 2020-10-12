package com.latticeengines.domain.exposed.ratings.coverage;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UpdateRatingCoverageRequest {

    @JsonProperty("counts")
    private Map<String, Long> counts;

    public Map<String, Long> getCounts() {
        return counts;
    }

    public void setCounts(Map<String, Long> counts) {
        this.counts = counts;
    }
}
