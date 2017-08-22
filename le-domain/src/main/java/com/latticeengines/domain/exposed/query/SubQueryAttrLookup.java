package com.latticeengines.domain.exposed.query;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class SubQueryAttrLookup extends Lookup {

    @JsonProperty("subquery")
    private SubQuery subQuery;

    @JsonProperty("attribute")
    private String attribute;

    private SubQueryAttrLookup() {}

    public SubQueryAttrLookup(SubQuery subQuery, String attribute) {
        this.subQuery = subQuery;
        this.attribute = attribute;
    }

    public SubQuery getSubQuery() {
        return subQuery;
    }

    public void setSubQuery(SubQuery subQuery) {
        this.subQuery = subQuery;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }
}
