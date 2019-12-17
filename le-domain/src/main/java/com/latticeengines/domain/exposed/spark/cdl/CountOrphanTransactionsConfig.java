package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CountOrphanTransactionsConfig extends SparkJobConfig {

    /**
     *
     */
    private static final long serialVersionUID = 4363625331304905629L;

    public static final String NAME = "countOrphanTransactions";

    @Override
    public int getNumTargets() {
        return 0;
    }

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    @JsonProperty("JoinKeys")
    private List<String> joinKeys;

    public List<String> getJoinKeys() {
        return joinKeys;
    }

    public void setJoinKeys(List<String> joinKeys) {
        this.joinKeys = joinKeys;
    }
}
