package com.latticeengines.domain.exposed.datacloud.statistics;

import java.util.Map;

public class AccountMasterCube {
    private Map<String, AttributeStatistics> statistics;

    public Map<String, AttributeStatistics> getStatistics() {
        return statistics;
    }

    public void setStatistics(Map<String, AttributeStatistics> statistics) {
        this.statistics = statistics;
    }
}
