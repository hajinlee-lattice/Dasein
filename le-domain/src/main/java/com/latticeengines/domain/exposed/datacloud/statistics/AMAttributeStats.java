package com.latticeengines.domain.exposed.datacloud.statistics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AMAttributeStats {

    @JsonProperty("UniqStats")
    private AttributeStats uniqueLocationBasedStatistics;

    @JsonProperty("RowStats")
    private AttributeStats rowBasedStatistics;

    public AttributeStats getUniqueLocationBasedStatistics() {
        return uniqueLocationBasedStatistics;
    }

    public void setUniqueLocationBasedStatistics(AttributeStats uniqueLocationBasedStatistics) {
        this.uniqueLocationBasedStatistics = uniqueLocationBasedStatistics;
    }

    public AttributeStats getRowBasedStatistics() {
        return rowBasedStatistics;
    }

    public void setRowBasedStatistics(AttributeStats rowBasedStatistics) {
        this.rowBasedStatistics = rowBasedStatistics;
    }
}
