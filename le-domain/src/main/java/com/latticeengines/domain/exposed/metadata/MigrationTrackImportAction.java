package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MigrationTrackImportAction {

    @JsonProperty("actions")
    private List<Long> actions = new ArrayList<>();

    /*
     * TODO - double check with Jinyang
     */

    public List<Long> getActions() {
        return actions;
    }

    public void setActions(List<Long> actions) {
        this.actions = actions;
    }
}
