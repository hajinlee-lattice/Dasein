package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;

public class MatchTransformerConfig extends TransformerConfig {

    @JsonProperty("MatchInput")
    private MatchInput matchInput;

    @JsonProperty("NewEntitiesTableName")
    private String newEntitiesTableName;

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
}
