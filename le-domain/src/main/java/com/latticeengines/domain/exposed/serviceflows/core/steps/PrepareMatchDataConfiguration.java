package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PrepareMatchDataConfiguration extends SparkJobStepConfiguration {

    @JsonProperty("input_table_name")
    private String inputTableName;

    @JsonProperty("match_group_id")
    private String matchGroupId;

    @JsonProperty("map_to_lattice_account")
    private boolean mapToLatticeAccount;

    public String getInputTableName() {
        return inputTableName;
    }

    public void setInputTableName(String inputTableName) {
        this.inputTableName = inputTableName;
    }

    public String getMatchGroupId() {
        return matchGroupId;
    }

    public void setMatchGroupId(String matchGroupId) {
        this.matchGroupId = matchGroupId;
    }

    public void setMapToLatticeAccount(boolean mapToLatticeAccount) {
        this.mapToLatticeAccount = mapToLatticeAccount;
    }

    public boolean isMapToLatticeAccount() {
        return mapToLatticeAccount;
    }
}
