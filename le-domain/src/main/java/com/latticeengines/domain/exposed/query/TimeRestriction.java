package com.latticeengines.domain.exposed.query;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TimeRestriction extends Restriction {
    @JsonProperty("Cmp")
    private ComparisonType comparisonType;

    @JsonProperty("Vals")
    private List<Object> values;

    public TimeRestriction() {}

    public TimeRestriction(ComparisonType comparisonType, List<Object> values) {
        this.comparisonType = comparisonType;
        this.values = values;
    }

    public ComparisonType getComparisonType() {
        return comparisonType;
    }

    public List<Object> getValues() {
        return values;
    }

}
