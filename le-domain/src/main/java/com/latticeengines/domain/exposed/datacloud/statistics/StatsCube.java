package com.latticeengines.domain.exposed.datacloud.statistics;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StatsCube {

    @JsonProperty("Stats")
    private Map<String, AttributeStats> statistics;

    @JsonProperty("Cnt")
    private Long count;

    public Map<String, AttributeStats> getStatistics() {
        return statistics;
    }

    public void setStatistics(Map<String, AttributeStats> statistics) {
        this.statistics = statistics;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

}
