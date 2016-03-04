package com.latticeengines.domain.exposed.propdata;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateCommandRequest {

    private String sourceTable;
    private String destTables;
    private String contractExternalID;
    private MatchCommandType commandType = MatchCommandType.MATCH_WITH_UNIVERSE;
    private Map<String, String> parameters;

    @JsonProperty("SourceTable")
    public String getSourceTable() {
        return sourceTable;
    }

    @JsonProperty("SourceTable")
    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    @JsonProperty("DestTables")
    public String getDestTables() {
        return destTables;
    }

    @JsonProperty("DestTables")
    public void setDestTables(String destTables) {
        this.destTables = destTables;
    }

    @JsonProperty("ContractExternalID")
    public String getContractExternalID() {
        return contractExternalID;
    }

    @JsonProperty("ContractExternalID")
    public void setContractExternalID(String contractExternalID) {
        this.contractExternalID = contractExternalID;
    }

    @JsonProperty("MatchCommandType")
    public MatchCommandType getCommandType() {
        return commandType;
    }

    @JsonProperty("MatchCommandType")
    public void setCommandType(MatchCommandType commandType) {
        this.commandType = commandType;
    }

    @JsonProperty("Parameters")
    public Map<String, String> getParameters() {
        return parameters;
    }

    @JsonProperty("Parameters")
    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }
}
