package com.latticeengines.domain.exposed.propdata;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SqoopRequest {

    private StageServer stageServer = StageServer.COLLECTION_DB;
    private String sqlTable, avroDir;

    @JsonProperty("StageServer")
    public StageServer getStageServer() {
        return stageServer;
    }

    @JsonProperty("StageServer")
    public void setStageServer(StageServer stageServer) {
        this.stageServer = stageServer;
    }

    @JsonProperty("SqlTable")
    public String getSqlTable() {
        return sqlTable;
    }

    @JsonProperty("SqlTable")
    public void setSqlTable(String sqlTable) {
        this.sqlTable = sqlTable;
    }

    @JsonProperty("AvroDir")
    public String getAvroDir() {
        return avroDir;
    }

    @JsonProperty("AvroDir")
    public void setAvroDir(String avroDir) {
        this.avroDir = avroDir;
    }
}
