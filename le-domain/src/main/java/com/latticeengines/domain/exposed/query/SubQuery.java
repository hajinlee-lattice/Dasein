package com.latticeengines.domain.exposed.query;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class SubQuery {

    @JsonProperty("query")
    private Query query;

    @JsonProperty("alias")
    private String alias;

    @JsonProperty("projections")
    private List<String> projections = new ArrayList<>();

    @JsonIgnore
    private Object subQueryExpression;

    public SubQuery() {
    }

    public SubQuery(Query query, String alias) {
        this.query = query;
        this.alias = alias;
    }

    public Query getQuery() {
        return query;
    }

    public void setQuery(Query query) {
        this.query = query;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public List<String> getProjections() {
        return projections;
    }

    public void setSubQueryExpression(Object subQueryExpression) {
        this.subQueryExpression = subQueryExpression;
    }

    public Object getSubQueryExpression() {
        return this.subQueryExpression;
    }

    public SubQuery withProjection(String attribute) {
        projections.add(attribute);
        return this;
    }
}
