package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;

public class MatchTransformerConfig extends TransformerConfig {

    @JsonProperty("MatchInput")
    private MatchInput matchInput;

    @JsonProperty("NewEntitiesTableName")
    private String newEntitiesTableName;

    @JsonProperty("RootOperationUID")
    private String rootOperationUid;

    public MatchInput getMatchInput() {
        return matchInput;
    }

    public void setMatchInput(MatchInput matchInput) {
        this.matchInput = matchInput;
    }

    public String getNewEntitiesTableName() {
        return newEntitiesTableName;
    }

    public void setNewEntitiesTableName(String newEntitiesTableName) {
        this.newEntitiesTableName = newEntitiesTableName;
    }

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public void setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
    }
}
