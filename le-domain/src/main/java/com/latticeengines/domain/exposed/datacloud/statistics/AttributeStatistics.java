package com.latticeengines.domain.exposed.datacloud.statistics;

public class AttributeStatistics {
    private AttributeStatsDetails uniqueLocationBasedStatistics;

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
