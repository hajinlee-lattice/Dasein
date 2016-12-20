package com.latticeengines.domain.exposed.datacloud.statistics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AttributeStatistics {
    @JsonProperty("UniqStats")
    private AttributeStatsDetails uniqueLocationBasedStatistics;

    @JsonProperty("RowStats")
    private AttributeStatsDetails rowBasedStatistics;

    public AttributeStatsDetails getUniqueLocationBasedStatistics() {
        return uniqueLocationBasedStatistics;
    }

    public void setUniqueLocationBasedStatistics(AttributeStatsDetails uniqueLocationBasedStatistics) {
        this.uniqueLocationBasedStatistics = uniqueLocationBasedStatistics;
    }

    public AttributeStatsDetails getRowBasedStatistics() {
        return rowBasedStatistics;
    }

    public void setRowBasedStatistics(AttributeStatsDetails rowBasedStatistics) {
        this.rowBasedStatistics = rowBasedStatistics;
    }
}
