package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class PurchaseRestriction extends Restriction {
    @JsonProperty("selector")
    private AggregationSelector selector;

    @JsonProperty("aggrType")
    private AggregationType aggregationType;

    @JsonProperty("Cmp")
    private ComparisonType comparisonType;

    @JsonProperty("Val")
    private Object value;

    PurchaseRestriction() { }

    public PurchaseRestriction(AggregationSelector selector,
                               AggregationType aggregationType,
                               ComparisonType comparisonType,
                               Object value) {
        this.selector = selector;
        this.aggregationType = aggregationType;
        this.comparisonType = comparisonType;
        this.value = value;
    }

    public AggregationSelector getSelector() {
        return selector;
    }

    public AggregationType getTransformationType() {
        return aggregationType;
    }

    public ComparisonType getComparisonType() {
        return comparisonType;
    }

    public Object getValue() {
        return value;
    }
}
