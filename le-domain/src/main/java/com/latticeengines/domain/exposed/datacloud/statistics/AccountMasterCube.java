package com.latticeengines.domain.exposed.datacloud.statistics;

import java.util.Map;

public class AccountMasterCube {
    private Dimensions dimensions;

    private Map<String, AttributeStatistics> statistics;

    public Dimensions getDimensions() {
        return dimensions;
    }

    public void setDimensions(Dimensions dimensions) {
        this.dimensions = dimensions;
    }

    public Map<String, AttributeStatistics> getStatistics() {
        return statistics;
    }

    public void setStatistics(Map<String, AttributeStatistics> statistics) {
        this.statistics = statistics;
    }
}
