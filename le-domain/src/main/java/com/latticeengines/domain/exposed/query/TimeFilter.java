package com.latticeengines.domain.exposed.query;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TimeFilter extends Restriction {

    @JsonIgnore
    private Lookup lhs;

    @JsonProperty("Cmp")
    private ComparisonType relation;

    @JsonProperty("Vals")
    private List<Object> values;

    public TimeFilter() {
    }

    public TimeFilter(ComparisonType relation, List<Object> values) {
        this(null, relation, values);
    }

    public TimeFilter(Lookup lhs, ComparisonType relation, List<Object> values) {
        this.lhs = lhs;
        this.relation = relation;
        this.values = values;
    }

    public ComparisonType getRelation() {
        return relation;
    }

    public List<Object> getValues() {
        return values;
    }

    public Lookup getLhs() {
        return lhs;
    }

    public void setLhs(Lookup lhs) {
        this.lhs = lhs;
    }

}
