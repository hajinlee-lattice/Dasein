package com.latticeengines.domain.exposed.propdata;


import com.fasterxml.jackson.annotation.JsonProperty;

public class CreateCommandRequest {

    private String sourceTable;
    private String destTables;
    private String contractExternalID;
    private MatchCommandType commandType = MatchCommandType.MATCH_WITH_UNIVERSE;

    @JsonProperty("SourceTable")
    public String getSourceTable() {
        return sourceTable;
    }

    @JsonProperty("SourceTable")
    public void setSourceTable(String sourceTable) {
        this.sourceTable = sourceTable;
    }

    @JsonProperty("DestTable")
    public String getDestTables() {
        return destTables;
    }

    @JsonProperty("DestTable")
    public void setDestTables(String destTables) {
        this.destTables = destTables;
    }

    @JsonProperty("ContractExternalID")
    public String getContractExternalID() { return contractExternalID; }

    @JsonProperty("ContractExternalID")
    public void setContractExternalID(String contractExternalID) { this.contractExternalID = contractExternalID; }

    @JsonProperty("MatchCommandType")
    public MatchCommandType getCommandType() { return commandType; }

    @JsonProperty("MatchCommandType")
    public void setCommandType(MatchCommandType commandType) { this.commandType = commandType; }
}
