package com.latticeengines.domain.exposed.query;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class SubQueryLookup extends Lookup {
    @JsonProperty("subquery")
    private SubQuery subQuery;

    private SubQueryLookup() {}

    public SubQueryLookup(SubQuery subQuery) {
        this.subQuery = subQuery;
    }

    public SubQuery getSubQuery() {
        return subQuery;
    }

    public void setSubQuery(SubQuery subQuery) {
        this.subQuery = subQuery;
    }

}
